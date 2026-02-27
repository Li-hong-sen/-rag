#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import json
from dotenv import load_dotenv
from pdf_image_extractor import extract_images_from_pdf, copy_images_to_server

def main():
    # 加载.env文件中的环境变量
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='处理PDF文件，提取图片并创建RAGFlow知识库')
    parser.add_argument('pdf_path', help='PDF文件路径')
    parser.add_argument('--api_key', help='RAGFlow API密钥（可选，默认从环境变量获取）')
    parser.add_argument('--image_dir', default='./images', help='本地图片存储目录，默认为./images')
    parser.add_argument('--mount_dir', default='/app/images', help='图片服务器容器内的挂载目录，默认为/app/images')
    parser.add_argument('--skip_ragflow', action='store_true', help='跳过创建RAGFlow知识库，只处理图片')
    parser.add_argument('--server_ip', help='图片服务器IP地址（可选，默认从环境变量RAGFLOW_SERVER_IP获取）')
    
    args = parser.parse_args()
    
    # 确保本地图片目录存在
    os.makedirs(args.image_dir, exist_ok=True)
    
    # 优先使用命令行参数的API密钥，其次使用环境变量
    api_key = args.api_key or os.getenv('RAGFLOW_API_KEY')

    # 获取PDF文件名用于MinIO bucket命名
    pdf_filename = os.path.basename(args.pdf_path)
    print(f"处理PDF文件: {pdf_filename}")
    
    try:
        # 提取图片和页面文档
        print(f"第1步：处理PDF并提取图片...")
        
        # 特殊处理：如果是挖掘机维修案例，强制定制Bucket名称
        custom_bucket_name = None
        if "挖掘机维修案例" in pdf_filename:
            custom_bucket_name = "ragflow-excavator-repair"
            print(f"检测到挖掘机案例，使用定制Bucket名称: {custom_bucket_name}")
            
        page_documents, extracted_images = extract_images_from_pdf(
            args.pdf_path, 
            pdf_filename=pdf_filename, 
            custom_bucket_name=custom_bucket_name
        )
        
        # 复制图片到图片服务器目录
        print(f"第2步：复制图片到服务器目录...")
        copy_images_to_server(extracted_images, args.image_dir)
        
        # 保存每个页面为独立的md文件
        print(f"第3步：为每个页面生成独立的md文件...")
        page_files = []
        base_name = os.path.splitext(os.path.basename(args.pdf_path))[0]
        
        for page_doc in page_documents:
            page_filename = f"{base_name}_page{page_doc['page']}.md"
            with open(page_filename, "w", encoding="utf-8") as f:
                f.write(page_doc['content'])
            page_files.append(page_filename)
            print(f"  - 已保存: {page_filename}")

        print(f"第4步：共生成{len(page_files)}个页面文件")
        
        # 如果设置了--skip_ragflow参数，跳过创建RAGFlow知识库
        if args.skip_ragflow:
            print("已跳过创建RAGFlow知识库")
            print("\n处理完成！")
            print(f"- 图片已保存到: {args.image_dir}")
            print(f"- 页面文件已保存: {', '.join(page_files)}")
            print("\n图片已自动上传到MinIO并设置为公开访问。")
            return
        
        # 检查API密钥
        if not api_key:
            print("错误：未提供RAGFlow API密钥，请通过--api_key参数或在.env文件中设置RAGFLOW_API_KEY")
            return
        
        print(f"第5步：创建RAGFlow知识库和助手...")
        
        # 使用多文档方式创建RAGFlow知识库和助手
        from ragflow_kb_manager import create_ragflow_resources_multi_docs
        # 准备自定义名称
        custom_dataset_name = None
        custom_assistant_name = None
        
        if "挖掘机维修案例" in pdf_filename:
            custom_dataset_name = "挖掘机维修助手（富文本增强）" # 虽然这里叫dataset_name，但通常用于知识库名
            custom_assistant_name = "挖掘机维修助手（富文本增强）"
            print(f"使用定制知识库/助手名称: {custom_assistant_name}")

        dataset, assistant = create_ragflow_resources_multi_docs(
            page_documents,
            page_files,
            args.pdf_path,
            api_key,
            base_url=os.getenv('RAGFLOW_BASE_URL', 'http://localhost:8080'),
            custom_dataset_name=custom_dataset_name,
            custom_assistant_name=custom_assistant_name
        )
        
        print(f"\n处理完成！")
        print(f"- 图片已保存到: {args.image_dir}")
        print(f"- 页面文件已保存: {', '.join(page_files)}")

        if dataset and assistant:
            print(f"- 知识库ID: {dataset.id}")
            print(f"- 聊天助手ID: {assistant.id}")
        else:
            print("- RAGFlow数据集创建失败，已跳过")
            print("- 您可以手动在RAGFlow界面上传增强文本文件")

        print("\n图片已自动上传到MinIO并设置为公开访问，无需额外配置。")
    except Exception as e:
        print(f"处理过程中出现错误：{str(e)}")
        # 保存已提取的图片和文本，即使处理过程中出错
        if 'extracted_images' in locals() and extracted_images:
            print(f"已提取{len(extracted_images)}张图片并保存到{args.image_dir}")
        if 'enhanced_text' in locals() and enhanced_text:
            text_filename = os.path.splitext(os.path.basename(args.pdf_path))[0] + "_enhanced.txt"
            with open(text_filename, "w", encoding="utf-8") as f:
                f.write(enhanced_text)
            print(f"增强文本已保存到{text_filename}")

if __name__ == "__main__":
    main() 