import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
import json
import re
import hashlib

# 加载环境变量
load_dotenv()

class MinIOClient:
    def __init__(self):
        """
        初始化MinIO客户端
        从环境变量读取配置：
        - MINIO_ENDPOINT: MinIO服务器地址
          * RAGFlow内置: http://minio:9000
          * 独立部署: http://localhost:9000
        - MINIO_ACCESS_KEY: 访问密钥 (RAGFlow默认: rag_flow)
        - MINIO_SECRET_KEY: 秘密密钥 (RAGFlow默认: infini_rag_flow)
        - MINIO_BUCKET_NAME: bucket名称 (可选，默认使用文档名)
        """
        self.endpoint = os.getenv('MINIO_ENDPOINT')
        self.access_key = os.getenv('MINIO_ACCESS_KEY')
        self.secret_key = os.getenv('MINIO_SECRET_KEY')
        self.bucket_name = os.getenv('MINIO_BUCKET_NAME', 'ragflow-images')

        if not all([self.endpoint, self.access_key, self.secret_key]):
            raise ValueError("缺少MinIO配置。请在.env文件中设置MINIO_ENDPOINT、MINIO_ACCESS_KEY和MINIO_SECRET_KEY")

        # 创建S3客户端 (MinIO兼容)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name='us-east-1'  # MinIO不需要真实的region
        )

        print("MinIO客户端初始化完成")
        print(f"端点: {self.endpoint}")
        print(f"Bucket: {self.bucket_name}")

    def create_bucket_if_not_exists(self, bucket_name=None):
        """
        如果bucket不存在则创建，并设置公开访问策略

        参数:
        - bucket_name: bucket名称，如果不提供则使用默认名称
        """
        bucket = bucket_name or self.bucket_name

        try:
            # 检查bucket是否存在
            self.s3_client.head_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' 已存在")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Bucket不存在，创建它
                print(f"创建Bucket '{bucket}'...")
                try:
                    # 对于MinIO，需要指定CreateBucketConfiguration
                    self.s3_client.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={'LocationConstraint': 'us-east-1'}
                    )
                    print(f"Bucket '{bucket}' 创建成功")
                except Exception as create_error:
                    print(f"创建Bucket失败: {create_error}")
                    raise
            else:
                print(f"检查Bucket状态失败: {e}")
                raise

        # 设置公开访问策略
        self.set_public_read_policy(bucket)
        return bucket

    def set_public_read_policy(self, bucket_name):
        """
        设置bucket的公开读取策略，允许所有人读取对象

        参数:
        - bucket_name: bucket名称
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }

        try:
            self.s3_client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(policy)
            )
            print(f"Bucket '{bucket_name}' 已设置为公开读取")
        except Exception as e:
            print(f"设置公开访问策略失败: {e}")
            # 有些MinIO配置可能不支持bucket policy，我们可以尝试其他方法
            print("尝试设置对象级别的公开访问...")

    def upload_image(self, file_path, object_key, bucket_name=None):
        """
        上传图片文件到MinIO

        参数:
        - file_path: 本地文件路径
        - object_key: 在MinIO中的对象键 (文件名)
        - bucket_name: bucket名称，可选

        返回:
        - 公开访问URL
        """
        bucket = bucket_name or self.bucket_name

        # 确保bucket存在
        self.create_bucket_if_not_exists(bucket)

        try:
            # 上传文件
            with open(file_path, 'rb') as file_data:
                self.s3_client.upload_fileobj(
                    file_data,
                    bucket,
                    object_key,
                    ExtraArgs={
                        'ContentType': 'image/png',  # 可以根据文件类型动态设置
                        'ACL': 'public-read'  # 设置为公开读取
                    }
                )

            # 生成公开访问URL
            public_url = f"{self.endpoint}/{bucket}/{object_key}"
            print(f"图片上传成功: {public_url}")
            return public_url

        except FileNotFoundError:
            raise FileNotFoundError(f"文件不存在: {file_path}")
        except Exception as e:
            print(f"上传失败: {e}")
            raise

    def upload_image_bytes(self, image_bytes, object_key, content_type='image/png', bucket_name=None):
        """
        直接上传图片字节数据到MinIO

        参数:
        - image_bytes: 图片的字节数据
        - object_key: 在MinIO中的对象键
        - content_type: 内容类型
        - bucket_name: bucket名称，可选

        返回:
        - 公开访问URL
        """
        bucket = bucket_name or self.bucket_name

        # 确保bucket存在
        self.create_bucket_if_not_exists(bucket)

        try:
            # 上传字节数据
            self.s3_client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=image_bytes,
                ContentType=content_type,
                ACL='public-read'
            )

            # 生成公开访问URL
            public_url = f"{self.endpoint}/{bucket}/{object_key}"
            print(f"图片上传成功: {public_url}")
            return public_url

        except Exception as e:
            print(f"上传失败: {e}")
            raise

    def get_bucket_url(self, bucket_name=None):
        """
        获取bucket的基础URL

        参数:
        - bucket_name: bucket名称，可选

        返回:
        - bucket的基础URL
        """
        bucket = bucket_name or self.bucket_name
        return f"{self.endpoint}/{bucket}"

    def list_objects(self, bucket_name=None, prefix=""):
        """
        列出bucket中的对象

        参数:
        - bucket_name: bucket名称，可选
        - prefix: 对象键前缀，用于过滤

        返回:
        - 对象列表
        """
        bucket = bucket_name or self.bucket_name

        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except Exception as e:
            print(f"列出对象失败: {e}")
            return []

# 全局MinIO客户端实例
_minio_client = None

def get_minio_client():
    """
    获取MinIO客户端单例实例

    返回:
    - MinIOClient实例
    """
    global _minio_client
    if _minio_client is None:
        try:
            _minio_client = MinIOClient()
        except Exception as e:
            print(f"初始化MinIO客户端失败: {e}")
            return None
    return _minio_client

def init_minio_bucket(pdf_filename=None, custom_bucket_name=None):
    """
    初始化MinIO bucket，为PDF文档创建专用bucket

    参数:
    - pdf_filename: PDF文件名，用于生成bucket名称
    - custom_bucket_name: 自定义bucket名称（可选，优先使用）

    返回:
    - bucket名称和基础URL
    """
    client = get_minio_client()
    if not client:
        return None, None

    # 如果提供了自定义bucket名称，直接使用
    if custom_bucket_name:
        bucket_name = custom_bucket_name
    # 否则，如果提供了PDF文件名，则使用它作为bucket名称的一部分
    elif pdf_filename:
        base_name = os.path.splitext(os.path.basename(pdf_filename))[0]
        
        # 1. 转换为小写
        safe_name = base_name.lower()
        
        # 2. 替换非法字符为横杠 (保留 a-z, 0-9, ., -, _)
        # regex matches ANY char that is NOT in the allowed set
        safe_name = re.sub(r'[^a-z0-9.\-_]', '-', safe_name)
        
        # 3. 如果结果为空或全是横杠（例如纯中文文件名），使用MD5哈希
        if not safe_name.replace('-', '').replace('_', '').replace('.', ''):
            name_hash = hashlib.md5(base_name.encode('utf-8')).hexdigest()[:8]
            bucket_name = f"ragflow-{name_hash}"
        else:
            # 去除首尾的特殊符号
            safe_name = safe_name.strip('.-_')
            # 避免连续的横杠
            safe_name = re.sub(r'-+', '-', safe_name)
            bucket_name = f"ragflow-{safe_name}"

        # 4. 确保长度合规 (最大255，但为了安全截取63字符)
        if len(bucket_name) > 63:
            bucket_name = bucket_name[:63]
            
    else:
        bucket_name = client.bucket_name

    try:
        client.create_bucket_if_not_exists(bucket_name)
        base_url = client.get_bucket_url(bucket_name)
        return bucket_name, base_url
    except Exception as e:
        print(f"初始化MinIO bucket失败: {e}")
        return None, None

if __name__ == "__main__":
    # 测试脚本
    print("测试MinIO客户端...")

    try:
        client = get_minio_client()
        if client:
            bucket_name, base_url = init_minio_bucket("test_document.pdf")
            print(f"测试成功！Bucket: {bucket_name}, URL: {base_url}")
        else:
            print("MinIO客户端初始化失败")
    except Exception as e:
        print(f"测试失败: {e}")
