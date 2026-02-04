import base64
import logging
from base64 import b64encode
from optparse import Option
from typing import Optional, List, Dict

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

    async def image_info(self, bucket_name: str, key: str) -> str:
        """
        调用 TOS image/info 接口获取图片文件信息
        api: https://www.volcengine.com/docs/6349/153631
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
        Returns:
            图片文件信息，json格式
        """

        query = {"x-tos-process": "image/info"}

        chunk_size = 69 * 1024

        response = None
        try:
            response = await self.get(
                bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                content_len = int(response.headers.get('content-length', "0"))
                if content_len > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, "
                        f"more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                return content.decode('utf-8')
            else:
                raise Exception(
                    f"get image info failed, "
                    f"tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()

    async def image_process(self, bucket_name: str, key: str,
                            process_uri: str,
                            saveas_object: Optional[str] = None,
                            saveas_bucket: Optional[str] = None) -> str:
        """
        调用 TOS 图片处理接口，直接使用 x-tos-process 参数
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
            process_uri: 图片处理的uri，例如 image/format,png 或 image/format,png/resize,w_100
            saveas_object: 指定保存的对象名称
            saveas_bucket: 指定保存的存储桶名称
        Returns:
            如果指定了saveas参数，则返回转存后的对象信息，json格式；
            否则返回处理后的图片文件，base64编码
        """
        query = {"x-tos-process": process_uri}

        if saveas_object:
            query["x-tos-save-object"] = base64.b64encode(
                saveas_object.encode('utf-8')).decode('ascii')
        if saveas_bucket:
            query["x-tos-save-bucket"] = base64.b64encode(
                saveas_bucket.encode('utf-8')).decode('ascii')

        chunk_size = 69 * 1024

        response = None
        try:
            response = await self.get(
                bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                content_len = int(response.headers.get('content-length', "0"))
                if content_len > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, "
                        f"more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                if saveas_object:
                    return content.decode('utf-8')
                else:
                    return base64.b64encode(content).decode()
            else:
                raise Exception(
                    f"image process failed, "
                    f"tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()

    async def image_format(self, bucket_name: str, key: str,
                           output_format: str,
                           saveas_object: Optional[str] = None,
                           saveas_bucket: Optional[str] = None) -> str:
        """
        调用 TOS image/format 接口对图片进行格式转换
        api: https://www.volcengine.com/docs/6349/153630
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
            output_format: 目标图片格式，可选值：jpg, png, webp, bmp, gif, tiff, heic
            saveas_object: 指定保存的对象名称，不指定则不转存，返回转换后的图片
            saveas_bucket: 指定保存的存储桶名称，不指定则默认使用当前存储桶
        Returns:
            如果指定了saveas参数，则返回转存后的对象信息，json格式；
            否则返回转换后的图片文件，base64编码
        """

        query = {"x-tos-process": f"image/format,{output_format}"}

        if saveas_object:
            query["x-tos-save-object"] = base64.b64encode(
                saveas_object.encode('utf-8')).decode('ascii')
        if saveas_bucket:
            query["x-tos-save-bucket"] = base64.b64encode(
                saveas_bucket.encode('utf-8')).decode('ascii')

        chunk_size = 69 * 1024

        response = None
        try:
            response = await self.get(
                bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                content_len = int(response.headers.get('content-length', "0"))
                if content_len > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, "
                        f"more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                if saveas_object:
                    return content.decode('utf-8')
                else:
                    return base64.b64encode(content).decode()
            else:
                raise Exception(
                    f"image format failed, "
                    f"tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()

    async def image_resize(self, bucket_name: str, key: str,
                           mode: Optional[str] = None,
                           width: Optional[int] = None,
                           height: Optional[int] = None,
                           long: Optional[int] = None,
                           short: Optional[int] = None,
                           limit: Optional[int] = None,
                           p: Optional[int] = None,
                           color: Optional[str] = None,
                           saveas_object: Optional[str] = None,
                           saveas_bucket: Optional[str] = None) -> str:
        """
        调用 TOS image/resize 接口对图片进行缩放
        api: https://www.volcengine.com/docs/6349/153626
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
            mode: 缩放模式，可选值：lfit, mfit, fixed, fill, pad
            width: 目标缩放图宽度
            height: 目标缩放图高度
            long: 目标缩放图长边
            short: 目标缩放图短边
            limit: 指定目标缩放图大于原图时是否进行缩放，1表示不缩放，0表示缩放
            p: 按比例缩放，取值范围0-1000
            color: 填充颜色，pad模式下使用，十六进制颜色码，如 000000
            saveas_object: 指定保存的对象名称，不指定则不转存，返回缩放后的图片
            saveas_bucket: 指定保存的存储桶名称，不指定则默认使用当前存储桶
        Returns:
            如果指定了saveas参数，则返回转存后的对象信息，json格式；
            否则返回缩放后的图片文件，base64编码
        """
        params = {}
        if p is not None:
            params["p"] = p
        else:
            if mode:
                params["m"] = mode
            if width is not None:
                params["w"] = width
            if height is not None:
                params["h"] = height
            if long is not None:
                params["l"] = long
            if short is not None:
                params["s"] = short
            if limit is not None:
                params["limit"] = limit
            if color:
                params["color"] = color

        if len(params) > 0:
            query = {"x-tos-process": "image/resize" + "".join(
                f",{k}_{v}" for k, v in params.items()
            )}
        else:
            query = {"x-tos-process": "image/resize"}

        if saveas_object:
            query["x-tos-save-object"] = base64.b64encode(
                saveas_object.encode('utf-8')).decode('ascii')
        if saveas_bucket:
            query["x-tos-save-bucket"] = base64.b64encode(
                saveas_bucket.encode('utf-8')).decode('ascii')

        chunk_size = 69 * 1024

        response = None
        try:
            response = await self.get(
                bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                content_len = int(response.headers.get('content-length', "0"))
                if content_len > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, "
                        f"more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                if saveas_object:
                    return content.decode('utf-8')
                else:
                    return base64.b64encode(content).decode()
            else:
                raise Exception(
                    f"image resize failed, "
                    f"tos server return: {response.json()}")
        finally:
            if response is not None:
                await response.aclose()

    async def image_watermark(self, bucket_name: str, key: str,
                              watermarks: List[dict],
                              saveas_object: Optional[str] = None,
                              saveas_bucket: Optional[str] = None) -> str:
        """
        调用 TOS image/watermark 接口为图片添加一个或多个水印
        api: https://www.volcengine.com/docs/6349/153627
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
            watermarks: 水印配置列表。每个配置项为一个字典，支持以下 key:
                - image: 水印图片的对象名称
                - image_process: 对水印图片进行的预处理操作，例如 "image/resize,p_30"
                - text: 文字水印内容
                - font_type: 文字水印字体 (wqy-zenhei, fangzhengkaiti等)
                - color: 文字颜色 (十六进制，如 000000)
                - size: 文字大小
                - shadow: 阴影透明度 [0, 100]
                - rotate: 顺时针旋转角度 [0, 360]
                - fill: 是否平铺满原图 (1 或 0)
                - transparency: 透明度 [0, 100]
                - gravity: 水印位置 (nw, north, ne, west, center, east, sw, south, se)
                - x: 水平边距
                - y: 垂直边距
                - voffset: 中线垂直偏移
                - p: 指定图片水印按照待添加水印的原图的比例进行缩放 (0, 1000]
                - order: 图文混合前后顺序 (0: 图片在前, 1: 文字在前)
                - align: 图文混合对齐方式 (0: 上, 1: 中, 2: 下)
                - interval: 图文混合间距
            saveas_object: 指定保存的对象名称，返回添加水印后的图片
            saveas_bucket: 指定保存的存储桶名称，默认使用当前存储桶
        Returns:
            如果指定了saveas参数，则返回转存后的对象信息，json格式；
            否则返回添加水印后的图片文件，base64编码
        """
        def url_safe_b64encode(s: str) -> str:
            return base64.urlsafe_b64encode(
                s.encode('utf-8')).decode('ascii').rstrip('=')

        actions = []
        for config in watermarks:
            params_list = []
            
            # 1. 优先处理 image/text/type 参数，它们通常需要放在前面
            image = config.get("image")
            if image:
                image_process = config.get("image_process")
                image_val = f"{image}?x-tos-process={image_process}" if image_process else image
                params_list.append(f"image_{url_safe_b64encode(image_val)}")
            
            text = config.get("text")
            if text:
                params_list.append(f"text_{url_safe_b64encode(text)}")
            
            font_type = config.get("font_type")
            if font_type:
                params_list.append(f"type_{url_safe_b64encode(font_type)}")

            # 2. 处理其他通用参数
            param_map = {
                "transparency": "t",
                "gravity": "g",
                "x": "x",
                "y": "y",
                "voffset": "voffset",
                "rotate": "rotate",
                "fill": "fill",
                "color": "color",
                "size": "size",
                "shadow": "shadow",
                "p": "P",  # 图片水印缩放使用大写 P
                "order": "order",
                "align": "align",
                "interval": "interval"
            }
            for k, v in param_map.items():
                if config.get(k) is not None:
                    params_list.append(f"{v}_{config[k]}")

            if params_list:
                # 按照文档要求，参数之间用逗号分隔
                action = "watermark," + ",".join(params_list)
                actions.append(action)

        # 多个 watermark 操作之间用斜杠 / 分隔，整体以 image/ 开头
        process_str = "image/" + "/".join(actions)
        query = {"x-tos-process": process_str}

        if saveas_object:
            query["x-tos-save-object"] = base64.b64encode(
                saveas_object.encode('utf-8')).decode('ascii')
        if saveas_bucket:
            query["x-tos-save-bucket"] = base64.b64encode(
                saveas_bucket.encode('utf-8')).decode('ascii')

        chunk_size = 69 * 1024
        response = None
        try:
            response = await self.get(
                bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                content_len = int(response.headers.get('content-length', "0"))
                if content_len > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, "
                        f"more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                if saveas_object:
                    return content.decode('utf-8')
                else:
                    return base64.b64encode(content).decode()
            else:
                raise Exception(
                    f"image watermark failed, "
                    f"tos server return: {response.json()}, "
                    f"x-tos-process: {process_str}")
        finally:
            if response is not None:
                await response.aclose()

    async def image_blind_watermark(self, bucket_name: str, key: str, text: str,
                                    version: Optional[int] = None, level: Optional[int] = None,
                                    saveas_object: Optional[str] = None,
                                    saveas_bucket: Optional[str] = None) -> str:
        """
        调用 TOS image/blindwatermark 接口为图片添加盲水印
        Args:
            bucket_name: 存储桶名称
            key: 对象名称
            text: 文字水印内容，必选参数，直接传入明文字符串即可（内部会自动进行 URL 安全的 Base64 编码）
            version: 版本号，可选参数，默认值为1，取值范围为[1, 2]
            level: 强度，可选参数，默认值为1，取值范围为[1, 2, 3]
            saveas_object: 指定保存的对象名称，返回添加水印后的图片
            saveas_bucket: 指定保存的存储桶名称，默认使用当前存储桶
        Returns:
            如果指定了saveas参数，则返回转存后的对象信息，json格式；
            否则返回添加水印后的图片文件，base64编码
        """
        params = {}
        # text 需要进行 url 安全的 base64 编码
        params["text"] = base64.urlsafe_b64encode(text.encode('utf-8')).decode('ascii').rstrip('=')

        if version is not None:
            params["version"] = version
        if level is not None:
            params["level"] = level

        query = {"x-tos-process": "image/blindwatermark" + "".join(
            f",{k}_{v}" for k, v in params.items()
        )}

        if saveas_object:
            query["x-tos-save-object"] = base64.b64encode(
                saveas_object.encode('utf-8')).decode('ascii')
        if saveas_bucket:
            query["x-tos-save-bucket"] = base64.b64encode(
                saveas_bucket.encode('utf-8')).decode('ascii')

        chunk_size = 69 * 1024

        response = None
        try:
            response = await self.get(
                bucket=bucket_name, key=key, params=query)
            if response.status_code == 200 or response.status_code == 206:
                content_len = int(response.headers.get('content-length', "0"))
                if content_len > self.max_object_size:
                    raise Exception(
                        f"Bucket: {bucket_name} object: {key} is too large, "
                        f"more than {self.max_object_size} bytes")

                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size):
                    content.extend(chunk)

                if saveas_object:
                    return content.decode('utf-8')
                else:
                    return base64.b64encode(content).decode()
            else:
                raise Exception(
                    f"image blind watermark failed, "
                    f"tos server return: {response.json()}")
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
