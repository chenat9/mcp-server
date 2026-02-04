import base64
import logging
from typing import Optional
import json

from mcp_server_tos.config import TosConfig
from mcp_server_tos.resources.service import TosResource

logger = logging.getLogger(__name__)


class ObjectResource(TosResource):
    """
        火山引擎TOS 对象资源操作类
    """

    def __init__(self, config: TosConfig):
        super(ObjectResource, self).__init__(config)
        self.max_object_size = config.max_object_size

    async def get_object(self, bucket_name: str, key: str) -> str:
        """
        调用 TOS GetObject 接口获取对象内容
        api: https://www.volcengine.com/docs/6349/74850
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
        Returns:
            对象内容
        """
        chunk_size = 69 * 1024  # Using same chunk size as example for proven performance

        response = None
        try:
            response = await self.get(bucket=bucket_name, key=key)
            if response.status_code == 200 or response.status_code == 206:
                if int(response.headers.get('content-length', "0")) > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                if is_text_file(key):
                    return content.decode('utf-8')
                else:
                    return base64.b64encode(content).decode()
            else:
                raise Exception(f"get object failed, tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()

    async def video_info(self, bucket_name: str, key: str) -> str:
        """
        调用 TOS video/info 接口获取视频文件信息
        api: https://www.volcengine.com/docs/6349/336156
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
        Returns:
            视频文件信息，json格式
        """

        query = {"x-tos-process": "video/info"}

        chunk_size = 69 * 1024  # Using same chunk size as example for proven performance

        response = None
        try:
            response = await self.get(bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                if int(response.headers.get('content-length', "0")) > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                return content.decode('utf-8')
            else:
                raise Exception(f"get video info failed, tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()

    async def video_snapshot(self, bucket_name: str, key: str, time: Optional[int] = None,
                             width: Optional[int] = None, height: Optional[int] = None, mode: Optional[str] = None,
                             output_format: Optional[str] = None, auto_rotate: Optional[str] = None,
                             saveas_object: Optional[str] = None, saveas_bucket: Optional[str] = None) -> str:
        """
        调用 TOS video/snapshot 接口对视频文件进行截帧
        api: https://www.volcengine.com/docs/6349/336155
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
            time: 指定在视频中截图时间点，单位为毫秒（ms）
            width: 指定截图宽度，如果指定为 0，则按原图的分辨率比例自动计算，单位为像素（px）
            height: 指定截图高度，如果指定为 0，则按原图的分辨率比例自动计算，单位为像素（px）
            mode: 指定截图模式，模式区别为：
                - 默认模式: 不指定即为默认模式，将根据时间精确截图。
                - fast: 截取该时间点之前的最近的一个关键帧。
            output_format: 指定截图格式，取值范围为：
                - jpg: JPEG 格式，默认值。
                - png: PNG 格式。
            auto_rotate: 指定是否自动旋转，取值范围为：
                - auto: 在截图生成之后根据视频信息进行自动旋转。
                - w: 在截图生成之后根据视频信息强制按照宽大于高的模式旋转。
                - h: 在截图生成之后根据视频信息强制按照高大于宽的模式旋转。
            saveas_object: 指定截图保存的对象名称，不指定则不转存，返回截帧后的图片
            saveas_bucket: 指定截图保存的存储桶名称，不指定则默认使用当前存储桶
        Returns:
            如果指定了saveas参数，则返回转存后的对象信息，json格式；否则返回截帧后的图片文件，jpg或png格式，base64编码
        """

        params = {}
        if time is not None:
            params["t"] = time
        if width is not None:
            params["w"] = width
        if height is not None:
            params["h"] = height
        if mode is not None:
            params["m"] = mode
        if output_format is not None:
            params["f"] = output_format
        if auto_rotate is not None:
            params["ar"] = auto_rotate

        if len(params) > 0:
            query = {"x-tos-process": "video/snapshot" + "".join(
                f",{k}_{v}" for k, v in params.items() if v is not None
            )}
        else:
            query = {"x-tos-process": "video/snapshot"}

        if saveas_object:
            query["x-tos-save-object"] = base64.b64encode(saveas_object.encode('utf-8')).decode('ascii')
        if saveas_bucket:
            query["x-tos-save-bucket"] = base64.b64encode(saveas_bucket.encode('utf-8')).decode('ascii')

        chunk_size = 69 * 1024  # Using same chunk size as example for proven performance

        response = None
        try:
            response = await self.get(bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                if int(response.headers.get('content-length', "0")) > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                if saveas_object:
                    return content.decode('utf-8')
                else:
                    return base64.b64encode(content).decode()
            else:
                raise Exception(f"get video snapshot failed, tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()


    async def text_to_image(self, task_type: str, input_data_source: dict, output_processing: dict, ai_model_config: dict):
        """
        调用 TOS TextToImage 接口，使用文生图模型生成图片
        api: Refer to info.md for detailed documentation
        Args:
            task_type: 任务类型，取值为 text_to_image
            input_data_source: 文生图模型提示词，包含 Prompt 字段
            input_processing: 图片前处理配置, 包含 "ProcessStyle" 和 "ProcessActions" 字段
            output_processing: 图片后处理和存储配置, 包含 "Bucket" and "Object" and "ForbiddenOverwrite"
            and "ProcessStyle" and "ProcessActions" fields
            model_config: 文生图模型配置, 包含 "ModelAction" and "ModelVersion" and "ReqJson" fields
        Returns:
            文生图模型的响应结果
        """
        # 构造请求参数
        request_body = {
            "TaskType": task_type,
            "InputDataSource": input_data_source,
            "OutputProcessing": output_processing,
            "ModelConfig": ai_model_config
        }

        # 调用 TOS TextToImage 接口，使用 aigc/text_to_image 处理参数
        response = None
        try:
            # 注意：此处的请求需要使用特殊的查询参数 x-tos-process=aigc/text_to_image
            response = await self.post(
                bucket=output_processing.get("Bucket"),
                key="",
                params={"x-tos-process": "aigc/text_to_image"},
                data=json.dumps(request_body)
            )

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"TextToImage failed, tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()

    async def image_to_image(self, source_bucket: str, task_type: str, input_data_source: dict, input_processing: dict,
                             output_processing: dict, ai_model_config: dict):
        """
        调用 TOS ImageToImage 接口，使用图生图模型生成图片
        api: Refer to info.md for detailed documentation
        Args:
            task_type: 任务类型，取值为 image_to_image
            input_data_source: 图生图模型输入，包含 Prompt 和 ImageUri 字段
            input_processing: 图片前处理配置, 包含 "ProcessStyle" 和 "ProcessActions" 字段
            output_processing: 图片后处理和存储配置, 包含 "Bucket" and "Object" and "ForbiddenOverwrite"
            and "ProcessStyle" and "ProcessActions" fields
            model_config: 图生图模型配置, 包含 "ModelAction" and "ModelVersion" and "ReqJson" fields
        Returns:
            图生图模型的响应结果
        """
        # 构造请求参数
        request_body = {
            "TaskType": task_type,
            "InputDataSource": input_data_source,
            "InputProcessing": input_processing,
            "OutputProcessing": output_processing,
            "ModelConfig": ai_model_config
        }

        # 调用 TOS ImageToImage 接口，使用 aigc/image_to_image 处理参数
        response = None
        try:
            response = await self.post(
                bucket=source_bucket,
                key="",
                params={"x-tos-process": "aigc/image_to_image"},
                data=json.dumps(request_body)
            )

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"ImageToImage failed, tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()


    async def image_understanding(self, bucket: str, key: str, model: str, prompt: str, detail: Optional[str] = None):
        """
        调用 TOS ImageUnderstanding 接口，对图片内容进行理解
        api: Refer to info.md for detailed documentation
        Args:
            bucket: 存储桶名称
            key: 图片对象的键
            model: 模型名称，如 doubao-seed-1.6-vision
            prompt: 提示词，用于指导图片理解的内容, 长度限制 1024
            detail: 控制理解精细度，可选值：auto, low, high，默认 auto
        Returns:
            图片理解的响应结果（JSON格式）
        """
        # 构造 x-tos-process 查询参数
        process_params = ["image/understanding"]

        if model:
            import base64
            # 对模型名称进行 base64 URL 编码
            encoded_model = base64.urlsafe_b64encode(model.encode('utf-8')).decode('ascii').rstrip('=')
            process_params.append(f"m_{encoded_model}")

        if prompt:
            import base64
            # 对提示词进行 base64 URL 编码
            encoded_prompt = base64.urlsafe_b64encode(prompt.encode('utf-8')).decode('ascii').rstrip('=')
            process_params.append(f"p_{encoded_prompt}")

        if detail and detail in ["auto", "low", "high"]:
            process_params.append(f"d_{detail}")

        process_str = ','.join(process_params)

        # 调用 GET 请求，使用 x-tos-process 查询参数
        response = None
        try:
            response = await self.get(
                bucket=bucket,
                key=key,
                params={"x-tos-process": process_str}
            )

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"ImageUnderstanding failed, tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()


    async def video_understanding(self, bucket: str, key: str, prompt: str,
                                model: Optional[str] = None, fps: Optional[float] = None):
        """
        调用 TOS VideoUnderstanding 接口，对视频内容进行理解
        api: Refer to info.md for detailed documentation
        Args:
            bucket: 存储桶名称
            key: 视频对象的键
            prompt: 提示词，用于指导视频理解的内容, 长度限制 1024
            model: 模型名称，如 doubao-seed-1.6-vision，可选
            fps: 每秒截多少帧，作为时序帧信息输入视频理解，取值范围 [0.2, 5]，默认 1.0，可选
        Returns:
            视频理解的响应结果（JSON格式）
        """
        # 构造 x-tos-process 查询参数
        process_params = ["video/understanding"]

        if model:
            import base64
            # 对模型名称进行 base64 URL 编码
            encoded_model = base64.urlsafe_b64encode(model.encode('utf-8')).decode('ascii').rstrip('=')
            process_params.append(f"m_{encoded_model}")

        if prompt:
            import base64
            # 对提示词进行 base64 URL 编码
            encoded_prompt = base64.urlsafe_b64encode(prompt.encode('utf-8')).decode('ascii').rstrip('=')
            process_params.append(f"p_{encoded_prompt}")

        if fps and 0.2 <= fps <= 5.0:
            process_params.append(f"fps_{fps}")

        process_str = ','.join(process_params)

        # 调用 GET 请求，使用 x-tos-process 查询参数
        response = None
        try:
            response = await self.get(
                bucket=bucket,
                key=key,
                params={"x-tos-process": process_str}
            )

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"VideoUnderstanding failed, tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()


def is_text_file(key: str) -> bool:
    """Determine if a file is text-based by its extension"""
    text_extensions = {
        '.txt', '.log', '.json', '.xml', '.yml', '.yaml', '.md',
        '.csv', '.ini', '.conf', '.py', '.js', '.html', '.css',
        '.sh', '.bash', '.cfg', '.properties'
    }
    return any(key.lower().endswith(ext) for ext in text_extensions)
