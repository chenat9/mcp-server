import base64
import json
import logging
import os
from typing import Optional, List

from mcp.server.session import ServerSession
from mcp.server.fastmcp import Context, FastMCP
from starlette.requests import Request

from mcp_server_tos.config import load_config, TosConfig, TOS_CONFIG, LOCAL_DEPLOY_MODE
from mcp_server_tos.credential import Credential
from mcp_server_tos.resources.bucket import BucketResource
from mcp_server_tos.resources.object import ObjectResource

logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("TOS MCP Server", host=os.getenv("MCP_SERVER_HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")))


def get_credential_from_request():
    ctx: Context[ServerSession, object] = mcp.get_context()
    raw_request: Request | None = ctx.request_context.request

    auth = None
    if raw_request:
        # 从 header 的 authorization 字段读取 base64 编码后的 sts json
        auth = raw_request.headers.get("authorization", None)
    if auth is None:
        # 如果 header 中没有认证信息，可能是 stdio 模式，尝试从环境变量获取
        auth = os.getenv("authorization", None)
    if auth is None:
        # 获取认证信息失败
        raise ValueError("Missing authorization info.")

    if ' ' in auth:
        _, base64_data = auth.split(' ', 1)
    else:
        base64_data = auth

    try:
        # 解码 Base64
        decoded_str = base64.b64decode(base64_data).decode('utf-8')
        data = json.loads(decoded_str)
        # 获取字段
        current_time = data.get('CurrentTime')
        expired_time = data.get('ExpiredTime')
        ak = data.get('AccessKeyId')
        sk = data.get('SecretAccessKey')
        session_token = data.get('SessionToken')
        if not ak or not sk or not session_token:
            raise ValueError("Invalid credentials ak, sk, session_token is null")

        return Credential(ak, sk, session_token, expired_time)
    except Exception as e:
        logger.error(f"Error get credentials: {str(e)}")
        raise


def get_tos_config() -> TosConfig:
    if TOS_CONFIG.deploy_mode == LOCAL_DEPLOY_MODE:
        return TOS_CONFIG
    else:
        credential = get_credential_from_request()
        return TosConfig(
            access_key=credential.access_key,
            secret_key=credential.secret_key,
            security_token=credential.security_token,
            region=TOS_CONFIG.region,
            endpoint=TOS_CONFIG.endpoint,
            deploy_mode=TOS_CONFIG.deploy_mode,
            max_object_size=TOS_CONFIG.max_object_size,
            buckets=[]
        )


@mcp.tool()
async def list_buckets():
    """
    List all buckets in TOS.
    Returns:
        A list of buckets.
    """
    try:
        config = get_tos_config()
        tos_resource = BucketResource(config)
        buckets = await tos_resource.list_buckets()
        return buckets
    except Exception:
        raise


@mcp.tool()
async def image_info(bucket_name: str, key: str):
    """
    Retrieves image file information from VolcEngine TOS by calling the image/info API.
    In the request, specify the bucket name and the full object key for the image.
    Args:
        bucket_name: The name of the bucket.
        key: The key of the object (image file).
    Returns:
        return the image file information in json format as string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.image_info(bucket_name, key)
        return content
    except Exception:
        raise


@mcp.tool()
async def image_process(bucket_name: str, key: str,
                        process_uri: str,
                        saveas_object: Optional[str] = None,
                        saveas_bucket: Optional[str] = None):
    """
    TOS image processing tool, directly uses the x-tos-process parameter.
    Args:
        bucket_name: The name of the bucket.
        key: The key of the object (image file).
        process_uri: Image processing URI. Can be a single operation like "image/format,png" 
                    or multiple operations like "image/format,png/resize,w_100".
        saveas_object: The object name to save the processed image as.
        saveas_bucket: The bucket name where the image should be saved.
    Returns:
        If saveas is specified, return the saveas object information in json format; 
        otherwise, return the processed image as a base64-encoded string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.image_process(bucket_name, key, process_uri,
                                                   saveas_object, saveas_bucket)
        return content
    except Exception:
        raise


@mcp.tool()
async def image_format(bucket_name: str, key: str, output_format: str,
                       saveas_object: Optional[str] = None, saveas_bucket: Optional[str] = None):
    """
    Converts the image format from VolcEngine TOS by calling the image/format API.
    Args:
        bucket_name: The name of the bucket.
        key: The key of the object (image file).
        output_format: Target image format, e.g., jpg, png, webp, bmp, gif, tiff, heic.
        saveas_object: The object name to save the converted image as.
        saveas_bucket: The bucket name where the image should be saved.
    Returns:
        If saveas is specified, return the saveas object information in json format; otherwise, return the converted image as a base64-encoded string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.image_format(bucket_name, key, output_format, saveas_object, saveas_bucket)
        return content
    except Exception:
        raise


@mcp.tool()
async def image_resize(bucket_name: str, key: str,
                       mode: Optional[str] = None, width: Optional[int] = None, height: Optional[int] = None,
                       long: Optional[int] = None, short: Optional[int] = None, limit: Optional[int] = None,
                       p: Optional[int] = None, color: Optional[str] = None,
                       saveas_object: Optional[str] = None, saveas_bucket: Optional[str] = None):
    """
    Resizes the image from VolcEngine TOS by calling the image/resize API.
    Args:
        bucket_name: The name of the bucket.
        key: The key of the object (image file).
        # Resize by width/height parameters
        mode: Resize mode. Valid values:
            - lfit: Scale proportionally to fit within dimensions (default)
            - mfit: Scale proportionally to fill dimensions, cropping if needed
            - fixed: Scale to exact dimensions without aspect ratio
            - fill: Scale proportionally to fit, fill space with color
            - pad: Scale proportionally to fit, pad remaining space with color
        width: Target width.
        height: Target height.
        long: Target long side.
        short: Target short side.
        limit: Whether to scale if target is larger than original.

        # Resize by percentage parameters
        p: Scale percentage (0-1000).
            - 100: Original size (no scaling)
            - 50: Scale to 50% of original size (50% smaller)
            - 200: Scale to 200% of original size (twice as large)
        color: Fill color for pad mode.
        saveas_object: The object name to save the resized image as.
        saveas_bucket: The bucket name where the image should be saved.
    Returns:
        If saveas is specified, return the saveas object information in json format; otherwise, return the resized image as a base64-encoded string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.image_resize(bucket_name, key, mode, width, height, long, short, limit, p, color,
                                                  saveas_object, saveas_bucket)
        return content
    except Exception:
        raise


@mcp.tool()
async def image_watermark(bucket_name: str, key: str,
                          watermarks: List[dict],
                          saveas_object: Optional[str] = None,
                          saveas_bucket: Optional[str] = None):
    """
    Adds one or more watermarks (text, image, or mixed) to an image in TOS.

    Args:
        bucket_name: Name of the bucket containing the image.
        key: Key of the original image object.
        watermarks: List of watermark configurations. Each config dict supports:
            - Basic Parameters:
                - transparency (t): Transparency [0, 100]. Default 100 (opaque).
                - gravity (g): Position (nw, north, ne, west, center, east, sw, south, se). Default 'se'.
                - x, y: Horizontal/Vertical margins (0-4096 pixels). Default 10.
                - voffset: Vertical offset for middle positions (-1000 to 1000 pixels). Default 0.
            - Image Watermark:
                - image: Object key of the watermark image (must be in the same bucket).
                - image_process: Pre-processing for the watermark image (e.g., "image/resize,p_30").
                - p (P): Scale percentage of the watermark relative to the base image (0-1000).
            - Text Watermark:
                - text: Plain text content for the watermark (max 64 bytes before encoding).
                - font_type (type): Font type. Options: "wqy-zenhei" (default), "wqy-microhei", 
                  "fangzhengshusong", "fangzhengkaiti", "fangzhengheiti", "fangzhengfangsong", 
                  "droidsansfallback".
                - color: Text color in hex (e.g., "000000" for black). Default "000000".
                - size: Font size in pixels (0-1000]. Default 40.
                - shadow: Text shadow transparency [0, 100]. Default 0 (no shadow).
                - rotate: Clockwise rotation angle [0, 360]. Default 0.
                - fill: 1 to tile the text across the image, 0 otherwise. Default 0.
            - Mixed Watermark (Both image and text provided):
                - order: Layout order. 0: Image first (default), 1: Text first.
                - align: Alignment. 0: Top, 1: Middle, 2: Bottom (default).
                - interval: Spacing between text and image (0-1000 pixels). Default 0.
        saveas_object: Target object key for persistence.
        saveas_bucket: Target bucket for persistence.

    Example (Multiple Watermarks):
    [
        {"image": "logo.png", "image_process": "image/resize,p_10", "gravity": "nw"},
        {"text": "Confidential", "color": "FF0000", "size": 60, "gravity": "center", "rotate": 45}
    ]
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.image_watermark(bucket_name, key, watermarks,
                                                     saveas_object, saveas_bucket)
        return content
    except Exception:
        raise


@mcp.tool()
async def image_blind_watermark(bucket_name: str, key: str, text: str,
                                version: Optional[int] = None, level: Optional[int] = None,
                                saveas_object: Optional[str] = None, saveas_bucket: Optional[str] = None):
    """
    Adds blind watermark to the image from VolcEngine TOS by calling the image/blindwatermark API.
    Args:
        bucket_name: The name of the bucket.
        key: The key of the object (image file).
        text: Blind watermark text content. Do NOT manually base64 encode this string; the tool handles encoding internally.
        version: Blind watermark version.
        level: Blind watermark level.
        saveas_object: The object name to save the watermarked image as.
        saveas_bucket: The bucket name where the image should be saved.
    Returns:
        If saveas is specified, return the saveas object information in json format; otherwise, return the watermarked image as a base64-encoded string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.image_blind_watermark(bucket_name, key, text, version, level,
                                                           saveas_object, saveas_bucket)
        return content
    except Exception:
        raise


@mcp.tool()
async def list_objects(bucket: str, prefix: Optional[str] = None, start_after: Optional[str] = None,
                       continuation_token: Optional[str] = None):
    """
    List all objects in a bucket.
    Args:
        bucket: The name of the bucket.
        prefix: The prefix to filter objects.
        start_after: The start after key to filter objects.
        continuation_token: The continuation token to filter objects.
    Returns:
        A list of objects.
    """
    try:
        config = get_tos_config()
        tos_resource = BucketResource(config)
        objects = await tos_resource.list_objects(bucket, prefix, start_after, continuation_token)
        return objects
    except Exception:
        raise


@mcp.tool()
async def get_object(bucket: str, key: str):
    """
    Retrieves an object from VolcEngine TOS. In the GetObject request, specify the full key name for the object.
    Args:
        bucket: The name of the bucket.
        key: The key of the object.
    Returns:
        If the object content is text format, return the content as string.
        If the object content is binary format, return the content as base64 encoded string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.get_object(bucket, key)
        return content
    except Exception:
        raise

@mcp.tool()
async def video_info(bucket_name: str, key: str):
    """
    Retrieves video file information from VolcEngine TOS by calling the video/info API.
    In the request, specify the bucket name and the full object key for the video.
    Args:
        bucket_name: The name of the bucket.
        key: The key of the object (video file).
    Returns:
        return the video file information in json format as string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.video_info(bucket_name, key)
        return content
    except Exception:
        raise

@mcp.tool()
async def video_snapshot(bucket_name: str, key: str, time: Optional[int] = None,
                         width: Optional[int] = None, height: Optional[int] = None, mode: Optional[str] = None,
                         output_format: Optional[str] = None, auto_rotate: Optional[str] = None,
                         saveas_object: Optional[str] = None, saveas_bucket: Optional[str] = None):
    """
    Retrieves a video snapshot from VolcEngine TOS by calling the video/snapshot API.
    In the request, specify the bucket name, the full object key of the video, and the snapshot parameters.
    Args:
        bucket_name: The name of the bucket.
        key: The key of the object (video file).
        time: The timestamp to capture the snapshot, in milliseconds (ms).
        width: The snapshot width in pixels (px). If set to 0, it is calculated automatically based on the original aspect ratio.
        height: The snapshot height in pixels (px). If set to 0, it is calculated automatically based on the original aspect ratio.
        mode: The snapshot mode. If not specified, the default mode captures the frame precisely at the given timestamp.
              If set to "fast", it captures the nearest keyframe before the specified timestamp.
        output_format: The output image format. Supported values:
            - jpg: JPEG format (default).
            - png: PNG format.
        auto_rotate: Whether to rotate automatically. Supported values:
            - auto: Automatically rotates the snapshot based on video metadata after it is generated.
            - w: Forces rotation to a landscape orientation (width > height) based on video metadata after it is generated.
            - h: Forces rotation to a portrait orientation (height > width) based on video metadata after it is generated.
        saveas_object: The object name to save the snapshot as. If not specified, it won’t be saved (no persistence); return the captured frame image.
        saveas_bucket: The bucket name where the snapshot should be saved. If not specified, the current bucket will be used.
    Returns:
        If saveas is specified, return the saveas object information in json format; otherwise, return the snapshot image (JPG or PNG) as a base64-encoded string.
    """
    try:
        config = get_tos_config()
        tos_resource = ObjectResource(config)
        content = await tos_resource.video_snapshot(bucket_name, key, time, width, height, mode, output_format,
                                                    auto_rotate, saveas_object, saveas_bucket)
        return content
    except Exception:
        raise