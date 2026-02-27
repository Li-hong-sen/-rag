from ragflow_sdk import RAGFlow
from ragflow_sdk.modules.chat import Chat
import os
import time
import sys

def create_ragflow_resources_multi_docs(page_documents, page_files, pdf_filename, api_key, base_url="http://localhost:8080", custom_dataset_name=None, custom_assistant_name=None):
    """
    使用多个独立页面文档创建RAGFlow知识库和聊天助手

    参数:
    - page_documents: 页面文档列表，每个元素包含page、content、title
    - page_files: 页面文件路径列表
    - pdf_filename: PDF文件名
    - api_key: RAGFlow API密钥
    - base_url: RAGFlow基础URL
    - custom_dataset_name: 自定义知识库名称 (可选)
    - custom_assistant_name: 自定义助手名称 (可选)
    """
    print(f"创建RAGFlow资源，使用{len(page_documents)}个独立页面文档")

    try:
        # 初始化RAGFlow客户端
        rag_object = RAGFlow(api_key=api_key, base_url=base_url)

        # 创建数据集名称
        if custom_dataset_name:
            dataset_name = custom_dataset_name
        else:
            base_name = os.path.splitext(os.path.basename(pdf_filename))[0]
            dataset_name = f"{base_name}_知识库"

        print(f"创建多文档数据集: {dataset_name}")

        # 删除已存在的知识库和助手
        try:
            # 查找并删除已存在的知识库
            existing_datasets = rag_object.list_datasets()
            dataset_ids_to_delete = []
            for ds in existing_datasets:
                if ds.name == dataset_name:
                    print(f"发现已存在的知识库 '{dataset_name}'，正在删除...")
                    dataset_ids_to_delete.append(ds.id)

            if dataset_ids_to_delete:
                rag_object.delete_datasets(dataset_ids_to_delete)
                print("已删除旧的知识库")

            # 查找并删除已存在的聊天助手
            if custom_assistant_name:
                assistant_name = custom_assistant_name
            else:
                base_name = os.path.splitext(os.path.basename(pdf_filename))[0]
                assistant_name = f"{base_name}_助手"
            existing_chats = rag_object.list_chats()
            chat_ids_to_delete = []
            for chat in existing_chats:
                if chat.name == assistant_name:
                    print(f"发现已存在的聊天助手 '{assistant_name}'，正在删除...")
                    chat_ids_to_delete.append(chat.id)

            if chat_ids_to_delete:
                rag_object.delete_chats(chat_ids_to_delete)
                print("已删除旧的聊天助手")

        except Exception as cleanup_e:
            print(f"清理已存在资源时出错: {str(cleanup_e)}")
            print("继续创建新的资源...")

        # 创建数据集（简化配置，让每个文档天然成为一个分块）
        dataset = rag_object.create_dataset(
            name=dataset_name,
            description="多页面维修案例文档，每页独立分块",
            embedding_model="BAAI/bge-large-zh-v1.5@BAAI",
            chunk_method="naive"
        )
        print(f"数据集 '{dataset_name}' 创建成功")

        print("配置多文档分块策略：每个文档独立分块")
        dataset.update({
            "parser_config": {
                "chunk_token_num": 1000,  # 大分块确保每页完整
                "html4excel": False,
                "raptor": {"use_raptor": False}
            }
        })
        print("数据集配置更新成功")
        print("  - 策略: 每个页面文档天然独立分块")
        print("  - 分块大小: 1000 tokens (确保单页完整)")

        # 准备多个页面文档进行批量上传
        docs_to_upload = []
        for page_doc, page_file in zip(page_documents, page_files):
            print(f"准备上传第{page_doc['page']}页文档: {page_file}")

            encoded_text = page_doc['content'].encode('utf-8')
            docs_to_upload.append({
                "display_name": page_file,
                "blob": encoded_text
            })

        # 批量上传所有页面文档
        print(f"批量上传{len(docs_to_upload)}个页面文档...")
        dataset.upload_documents(docs_to_upload)
        print("所有页面文档上传成功")

        # 等待文档解析完成
        print("开始解析文档...")
        docs = dataset.list_documents()
        doc_ids = [doc.id for doc in docs if hasattr(doc, 'id')]
        print(f"开始解析文档，ID: {doc_ids}")

        if doc_ids:
            dataset.async_parse_documents(doc_ids)
            print("文档上传成功，正在解析...")

            # 等待文档解析完成
            all_done = False
            max_wait_time = 300  # 最长等待5分钟
            start_time = time.time()

            while not all_done and (time.time() - start_time) < max_wait_time:
                all_done = True
                for doc_id in doc_ids:
                    docs_check = dataset.list_documents(id=doc_id)
                    if docs_check and len(docs_check) > 0:
                        doc_status = docs_check[0].run
                        print(f"文档 {doc_id} 状态: {doc_status}")
                        if doc_status != "DONE":
                            all_done = False
                    else:
                        all_done = False

                if not all_done:
                    print("文档仍在解析中，等待10秒...")
                    time.sleep(10)

            print("文档解析完成！")

            # 检查最终分块数
            total_chunks = 0
            for doc_id in doc_ids:
                try:
                    chunks = dataset.get_chunks(document_id=doc_id)
                    chunk_count = len(chunks) if chunks else 0
                    total_chunks += chunk_count
                    print(f"文档 {doc_id} 解析出 {chunk_count} 个分块")
                except:
                    print(f"无法获取文档 {doc_id} 的分块信息")
            print(f"总共解析出 {total_chunks} 个分块")

        # 创建聊天助手
        if custom_assistant_name:
            assistant_name = custom_assistant_name
        else:
            base_name = os.path.splitext(os.path.basename(pdf_filename))[0]
            assistant_name = f"{base_name}_助手"
        print(f"创建聊天助手: {assistant_name}")

        assistant = rag_object.create_chat(
            name=assistant_name,
            dataset_ids=[dataset.id]
        )


        prompt_template = """
# 挖掘机维修专家

你是一位经验丰富的挖掘机维修技术专家，专门解答挖掘机维修问题。请仔细阅读下方的知识库内容，并结合上下文回答用户问题。

## 核心要求

### 📋 回答结构
1. **问题识别**：确认故障类型和设备型号
2. **案例引用**：关联相关维修案例
3. **诊断步骤**：按简单到复杂顺序说明排查方法
4. **维修方案**：提供具体操作步骤和所需零件
5. **预防建议**：说明如何避免类似故障

### 🖼️ 图片链接处理（极其重要）
- **严格要求**：在回答中必须完整、原封不动地输出案例中的图片链接
- **格式保持**：<img src="http://localhost:9000/ragflow-demo/xxx.png" alt="维修图片" width="300">
- **禁止修改**：不得更改URL、文件名或任何参数
- **必要引用**：涉及维修步骤时，必须引用对应图片

### ⚠️ 重要提醒
- 只基于知识库案例回答，不做主观推测
- 如果知识库中没有答案，请明确告知。
- 强调安全第一，提醒专业人员操作

Here is the knowledge base:
{knowledge}
The above is the knowledge base.
        """

        # 更新助手配置，参考ragflow_build.py的配置
        assistant.update({
            "prompt": {
                "prompt": prompt_template.strip(),
                "show_quote": True,  # 保持显示引用
                "top_n": 8  # 检索更多相关内容
            }
        })

        print(f"聊天助手 '{assistant_name}' 创建并配置完成")

        return dataset, assistant

    except Exception as e:
        print(f"创建RAGFlow多文档资源时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        raise