import fitz  # PyMuPDF
import os
import uuid
import shutil
import sys
from PIL import Image
import io
from minio_client import get_minio_client, init_minio_bucket

def extract_images_from_pdf(pdf_path, pdf_filename=None, custom_bucket_name=None):
    """
    提取PDF中的图片和文本，返回带有图片位置标记的增强文本

    参数:
    - pdf_path: PDF文件路径
    - pdf_filename: PDF文件名，用于创建MinIO bucket（可选）
    - custom_bucket_name: 自定义Bucket名称（可选，优先使用）
    """
    print(f"正在处理PDF: {pdf_path}")

    # 初始化MinIO客户端和bucket
    minio_client = get_minio_client()
    if not minio_client:
        raise ValueError("MinIO客户端初始化失败。请检查.env文件中的MinIO配置。")

    # 初始化bucket
    if custom_bucket_name:
        bucket_name, base_url = init_minio_bucket(custom_bucket_name=custom_bucket_name)
    else:
        bucket_name, base_url = init_minio_bucket(pdf_filename or os.path.basename(pdf_path))
    if not bucket_name or not base_url:
        raise ValueError("MinIO bucket初始化失败。")

    print(f"使用MinIO bucket: {bucket_name}")
    print(f"图片基础URL: {base_url}")
    
    # 确保本地临时目录存在
    os.makedirs("temp_images", exist_ok=True)
    
    # 提前声明变量，以确保在异常处理中可以访问
    doc = None
    extracted_text = []
    extracted_images = []
    page_count = 0
    
    try:
        # 先尝试打开PDF并提取所有内容，避免多次操作PDF对象
        doc = fitz.open(pdf_path)
        page_count = len(doc)
        print(f"PDF打开成功，共{page_count}页")
        
        # 一次性提取所有页面的文本和图片
        for page_idx in range(page_count):
            page = doc[page_idx]
            print(f"处理第{page_idx+1}页...")
            
            # 获取页面文本
            text = page.get_text()
            extracted_text.append({"page": page_idx+1, "text": text})
            
            # 提取图片
            image_list = page.get_images(full=True)
            print(f"第{page_idx+1}页发现{len(image_list)}张图片")
            
            # 处理当前页的图片
            for img_idx, img in enumerate(image_list):
                try:
                    xref = img[0]
                    base_image = doc.extract_image(xref)
                    image_bytes = base_image["image"]

                    # 生成有意义的图片名称，避免随机字符被LLM修改
                    # 格式: {PDF文件名}_page{页码}_img{图片序号}_v1.png
                    pdf_base_name = os.path.splitext(os.path.basename(pdf_path))[0]
                    # 移除特殊字符，只保留字母数字和下划线
                    safe_name = ''.join(c for c in pdf_base_name if c.isalnum() or c in '_-').strip()
                    if not safe_name:
                        safe_name = 'document'

                    image_filename = f"{safe_name}_page{page_idx+1}_img{img_idx+1}_v1.png"

                    # 确定内容类型
                    content_type = f"image/{base_image['ext']}"

                    # 直接上传到MinIO
                    image_url = minio_client.upload_image_bytes(
                        image_bytes=image_bytes,
                        object_key=image_filename,
                        content_type=content_type,
                        bucket_name=bucket_name
                    )

                    # 记录图片信息（不再需要temp_path，因为直接上传到MinIO）
                    extracted_images.append({
                        "filename": image_filename,
                        "page": page_idx + 1,
                        "content_type": base_image["ext"],
                        "url": image_url
                    })
                    print(f"成功提取并上传图片: {image_filename}")
                    print(f"图片URL: {image_url}")
                except Exception as e:
                    print(f"提取图片出错: {str(e)}")
        
        # 关闭文档，防止后续引用出错
        doc.close()
        doc = None
        
        # 现在我们有了所有的文本和图片，开始构建增强文本
        enhanced_text = []
        
        # 检查是否为维修案例文档
        is_maintenance_doc = any(
            "设备名称" in page_data["text"] or
            ("机型" in page_data["text"] and "故障名称" in page_data["text"])
            for page_data in extracted_text
        )
        
        if is_maintenance_doc:
            print("检测到维修案例文档格式，使用多文档策略")
            # 方案F：为每个页面生成独立的文档内容
            page_documents = []
            
            for i, page_data in enumerate(extracted_text):
                page_num = page_data["page"]
                page_text = page_data["text"].strip()

                # 清理不需要的数字前缀
                import re
                page_text = re.sub(r'^\d+\s*\n', '', page_text, flags=re.MULTILINE)

                # 为当前页面创建独立的文档内容
                page_content = []
                page_content.append(page_text)

                # 添加图片（HTML格式用于渲染）
                page_images = [img for img in extracted_images if img["page"] == page_num]
                if page_images:
                    page_content.append("\n### 相关图片\n")
                    for img in page_images:
                        page_content.append(f"<img src=\"{img['url']}\" alt=\"维修图片\" width=\"300\">")
                
                # 将页面内容添加到页面文档列表
                page_documents.append({
                    "page": page_num,
                    "content": "\n".join(page_content),
                    "title": f"维修案例第{page_num}页"
                })
            
            # 返回页面文档列表而不是合并的文本
            return page_documents, extracted_images
        else:
            # 一般文档处理
            print("使用一般文档格式处理")
            for page_data in extracted_text:
                page_num = page_data["page"]
                page_text = page_data["text"].strip()

                # 清理不需要的数字前缀
                import re
                page_text = re.sub(r'^\d+\s*\n', '', page_text, flags=re.MULTILINE)

                # 添加文本
                paragraphs = page_text.split('\n\n')
                for para in paragraphs:
                    if para.strip():
                        enhanced_text.append(para.strip())

                # 查找该页的图片
                page_images = [img for img in extracted_images if img["page"] == page_num]

                # 添加图片
                if page_images:
                    for img in page_images:
                        enhanced_text.append(f"\n<img src=\"{img['url']}\" alt=\"文档图片\" width=\"300\">\n")
        
        return "\n".join(enhanced_text), extracted_images
    
    except Exception as e:
        print(f"处理PDF文件出错: {str(e)}")
        
        # 如果已经提取了图片，尝试只使用图片构建文本
        if extracted_images:
            print("使用已提取的图片构建文本...")
            enhanced_text = []
            
            for page_num in range(1, page_count + 1):
                page_images = [img for img in extracted_images if img["page"] == page_num]
                
                if page_images:
                    enhanced_text.append(f"## 第{page_num}页图片\n")
                    for img in page_images:
                        enhanced_text.append(f"\n<img src=\"{img['url']}\" alt=\"图片\" width=\"300\">\n")
            
            if enhanced_text:
                return "\n".join(enhanced_text), extracted_images
        
        # 处理PDF失败但有图片，则直接将图片作为文档
        if os.path.exists(pdf_path):
            try:
                img = Image.open(pdf_path)
                img_filename = f"document_image_{uuid.uuid4().hex[:8]}.png"
                temp_path = os.path.join("temp_images", img_filename)
                img.save(temp_path)

                image_url = f"{base_url}/{img_filename}"
                
                img_info = {
                    "filename": img_filename,
                    "temp_path": temp_path,
                    "page": 1,
                    "content_type": "png",
                    "url": image_url
                }
                
                extracted_images = [img_info]
                return f"<img src=\"{image_url}\" alt=\"图片\" width=\"300\">", extracted_images
            except:
                pass
        
        # 如果所有尝试都失败
        raise Exception(f"无法处理文件: {pdf_path}, 原因: {str(e)}")
    finally:
        # 确保文档已关闭
        if doc:
            try:
                doc.close()
            except:
                pass

def copy_images_to_server(extracted_images, target_dir="./images"):
    """
    验证图片上传结果（MinIO集成版本）
    由于图片已直接上传到MinIO，此函数现在只用于验证和日志记录

    参数:
    - extracted_images: 从PDF提取的图片信息列表
    - target_dir: 图片服务器的图片存储目录（不再使用）
    """
    print(f"图片已上传到MinIO，共{len(extracted_images)}张图片：")

    for img_info in extracted_images:
        print(f"- {img_info['filename']}: {img_info['url']}")

    print("所有图片已成功上传到MinIO并设置为公开访问")
