import random
import uuid
import json
import base64
import time
import queue
import asyncio
import traceback
from asyncio import Task
import websockets
import os
from datetime import datetime
from core.providers.tts.base import TTSProviderBase
from core.providers.tts.dto.dto import SentenceType, ContentType, InterfaceType
from core.utils.tts import MarkdownCleaner
from core.utils import opus_encoder_utils, textUtils
from config.logger import setup_logging

TAG = __name__
logger = setup_logging()


class TTSProvider(TTSProviderBase):
    def __init__(self, config, delete_audio_file):
        super().__init__(config, delete_audio_file)

        # 设置为流式接口类型
        self.interface_type = InterfaceType.DUAL_STREAM

        # 基础配置
        self.appkey = config.get("appkey")
        self.format = config.get("format", "pcm")
        self.audio_file_type = config.get("format", "pcm")

        # 采样率配置
        sample_rate = config.get("sample_rate", "16000")
        self.sample_rate = int(sample_rate) if sample_rate else 16000

        # 音色配置 - CosyVoice大模型音色
        if config.get("private_voice"):
            self.voice = config.get("private_voice")
        else:
            self.voice = config.get("voice", "longxiaochun")  # CosyVoice默认音色

        # 音频参数配置
        volume = config.get("volume", "50")
        self.volume = int(volume) if volume else 50

        speech_rate = config.get("speech_rate", "0")
        self.speech_rate = int(speech_rate) if speech_rate else 0

        pitch_rate = config.get("pitch_rate", "0")
        self.pitch_rate = int(pitch_rate) if pitch_rate else 0

        # WebSocket配置
        self.host = config.get("host", "localhost")  # 默认本地地址
        self.ws_url = f"ws://{self.host}"
        self.ws = None
        self._monitor_task = None
        self.last_active_time = None

        # 专属tts设置
        self.task_id = uuid.uuid4().hex

        # 创建Opus编码器
        self.opus_encoder = opus_encoder_utils.OpusEncoderUtils(
            sample_rate=16000, channels=1, frame_size_ms=60
        )

    async def _ensure_connection(self):
        """确保WebSocket连接可用"""
        try:
            if self.ws:
                self.task_id = uuid.uuid4().hex
                logger.bind(tag=TAG).info(
                    f"使用已有链接..., task_id: {self.task_id}")
                return self.ws
            logger.bind(tag=TAG).debug("开始建立新连接...")

            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10,
            )
            self.task_id = uuid.uuid4().hex
            logger.bind(tag=TAG).debug(
                f"WebSocket连接建立成功, task_id: {self.task_id}")
            self.last_active_time = time.time()
            return self.ws
        except Exception as e:
            logger.bind(tag=TAG).error(f"建立连接失败: {str(e)}")
            self.ws = None
            self.last_active_time = None
            raise

    def tts_text_priority_thread(self):
        """流式文本处理线程"""
        while not self.conn.stop_event.is_set():
            try:
                message = self.tts_text_queue.get(timeout=1)
                logger.bind(tag=TAG).debug(
                    f"收到TTS任务｜{message.sentence_type.name} ｜ {message.content_type.name} | 会话ID: {self.conn.sentence_id}"
                )

                if message.sentence_type == SentenceType.FIRST:
                    self.conn.client_abort = False

                if self.conn.client_abort:
                    logger.bind(tag=TAG).info("收到打断信息，终止TTS文本处理线程")
                    continue

                if message.sentence_type == SentenceType.FIRST:
                    # 初始化参数
                    try:
                        logger.bind(tag=TAG).debug("开始启动TTS会话...")
                        future = asyncio.run_coroutine_threadsafe(
                            self.start_session(self.task_id),
                            loop=self.conn.loop,
                        )
                        future.result()
                        self.before_stop_play_files.clear()
                        logger.bind(tag=TAG).debug("TTS会话启动成功")

                    except Exception as e:
                        logger.bind(tag=TAG).error(f"启动TTS会话失败: {str(e)}")
                        continue

                elif ContentType.TEXT == message.content_type:
                    if message.content_detail:
                        try:
                            logger.bind(tag=TAG).debug(
                                f"开始发送TTS文本: {message.content_detail}"
                            )
                            future = asyncio.run_coroutine_threadsafe(
                                self.text_to_speak(
                                    message.content_detail, None),
                                loop=self.conn.loop,
                            )
                            future.result()
                            logger.bind(tag=TAG).debug("TTS文本发送成功")
                        except Exception as e:
                            logger.bind(tag=TAG).error(f"发送TTS文本失败: {str(e)}")
                            continue

                elif ContentType.FILE == message.content_type:
                    logger.bind(tag=TAG).info(
                        f"添加音频文件到待播放列表: {message.content_file}"
                    )
                    if message.content_file and os.path.exists(message.content_file):
                        # 先处理文件音频数据
                        self._process_audio_file_stream(message.content_file, callback=lambda audio_data: self.handle_audio_file(
                            audio_data, message.content_detail))
                if message.sentence_type == SentenceType.LAST:
                    try:
                        logger.bind(tag=TAG).debug("开始结束TTS会话...")
                        future = asyncio.run_coroutine_threadsafe(
                            self.finish_session(self.task_id),
                            loop=self.conn.loop,
                        )
                        future.result()
                    except Exception as e:
                        logger.bind(tag=TAG).error(f"结束TTS会话失败: {str(e)}")
                        continue

            except queue.Empty:
                continue
            except Exception as e:
                logger.bind(tag=TAG).error(
                    f"处理TTS文本失败: {str(e)}, 类型: {type(e).__name__}, 堆栈: {traceback.format_exc()}"
                )

    async def text_to_speak(self, text, _):
        try:
            if self.ws is None:
                logger.bind(tag=TAG).warning(f"WebSocket连接不存在，终止发送文本")
                return
            filtered_text = MarkdownCleaner.clean_markdown(text)
            run_request = {
                "header": {
                    "message_id": uuid.uuid4().hex,
                    "task_id": self.task_id,
                    "namespace": "FlowingSpeechSynthesizer",
                    "name": "RunSynthesis",
                    "appkey": self.appkey,
                },
                "payload": {"text": filtered_text},
            }
            await self.ws.send(json.dumps(run_request))
            self.last_active_time = time.time()
            return

        except Exception as e:
            logger.bind(tag=TAG).error(f"发送TTS文本失败: {str(e)}")
            if self.ws:
                try:
                    await self.ws.close()
                except:
                    pass
                self.ws = None
            raise

    async def start_session(self, task_id):
        logger.bind(tag=TAG).debug("开始会话～～")
        try:
            # 会话开始时检测上个会话的监听状态
            if (
                self._monitor_task is not None
                and isinstance(self._monitor_task, Task)
                and not self._monitor_task.done()
            ):
                logger.bind(tag=TAG).info(
                    "检测到未完成的上个会话，关闭监听任务和连接..."
                )
                await self.close()

            # 建立新连接
            await self._ensure_connection()

            # 启动监听任务
            self._monitor_task = asyncio.create_task(
                self._start_monitor_tts_response())

            start_request = {
                "header": {
                    "message_id": uuid.uuid4().hex,
                    "task_id": self.task_id,
                    "namespace": "FlowingSpeechSynthesizer",
                    "name": "StartSynthesis",
                    "appkey": self.appkey,
                },
                "payload": {
                    "voice": self.voice,
                    "format": self.format,
                    "sample_rate": self.sample_rate,
                    "volume": self.volume,
                    "speech_rate": self.speech_rate,
                    "pitch_rate": self.pitch_rate,
                    "enable_subtitle": True,
                },
            }
            await self.ws.send(json.dumps(start_request))
            self.last_active_time = time.time()
            logger.bind(tag=TAG).debug("会话启动请求已发送")
        except Exception as e:
            logger.bind(tag=TAG).error(f"启动会话失败: {str(e)}")
            # 确保清理资源
            await self.close()
            raise

    async def finish_session(self, task_id):
        logger.bind(tag=TAG).debug(f"关闭会话～～{task_id}")
        try:
            if self.ws:
                stop_request = {
                    "header": {
                        "message_id": uuid.uuid4().hex,
                        "task_id": self.task_id,
                        "namespace": "FlowingSpeechSynthesizer",
                        "name": "StopSynthesis",
                        "appkey": self.appkey,
                    }
                }
                await self.ws.send(json.dumps(stop_request))
                logger.bind(tag=TAG).debug("会话结束请求已发送")
                self.last_active_time = time.time()
                if self._monitor_task:
                    try:
                        await self._monitor_task
                    except Exception as e:
                        logger.bind(tag=TAG).error(
                            f"等待监听任务完成时发生错误: {str(e)}"
                        )
                    finally:
                        self._monitor_task = None
        except Exception as e:
            logger.bind(tag=TAG).error(f"关闭会话失败: {str(e)}")
            # 确保清理资源
            await self.close()
            raise

    async def close(self):
        """资源清理"""
        if self._monitor_task:
            try:
                self._monitor_task.cancel()
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.bind(tag=TAG).warning(f"关闭时取消监听任务错误: {e}")
            self._monitor_task = None

        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
            self.ws = None
            self.last_active_time = None

    async def _start_monitor_tts_response(self):
        """监听TTS响应"""
        try:
            session_finished = False  # 标记会话是否正常结束
            while not self.conn.stop_event.is_set():
                try:
                    msg = await self.ws.recv()
                    self.last_active_time = time.time()
                    # 检查客户端是否中止
                    if self.conn.client_abort:
                        logger.bind(tag=TAG).info("收到打断信息，终止监听TTS响应")
                        break
                    if isinstance(msg, str):  # 文本控制消息
                        try:
                            data = json.loads(msg)
                            header = data.get("header", {})
                            event_name = header.get("name")
                            if event_name == "SynthesisStarted":
                                logger.bind(tag=TAG).debug("TTS合成已启动")
                                self.tts_audio_queue.put(
                                    (SentenceType.FIRST, [], None)
                                )
                            elif event_name == "SentenceEnd":
                                # 发送缓存的数据
                                if self.conn.tts_MessageText:
                                    logger.bind(tag=TAG).info(
                                        f"句子语音生成成功： {self.conn.tts_MessageText}"
                                    )
                                    self.tts_audio_queue.put(
                                        (SentenceType.FIRST, [],
                                         self.conn.tts_MessageText)
                                    )
                                    self.conn.tts_MessageText = None
                            elif event_name == "SynthesisCompleted":
                                logger.bind(tag=TAG).debug(f"会话结束～～")
                                self._process_before_stop_play_files()
                                session_finished = True
                                break
                        except json.JSONDecodeError:
                            logger.bind(tag=TAG).warning("收到无效的JSON消息")
                    # 二进制消息（音频数据）
                    elif isinstance(msg, (bytes, bytearray)):
                        self.opus_encoder.encode_pcm_to_opus_stream(
                            msg, False, self.handle_opus)
                except websockets.ConnectionClosed:
                    logger.bind(tag=TAG).warning("WebSocket连接已关闭")
                    break
                except Exception as e:
                    logger.bind(tag=TAG).error(
                        f"处理TTS响应时出错: {e}\n{traceback.format_exc()}"
                    )
                    break
            # 仅在连接异常时才关闭
            if not session_finished and self.ws:
                try:
                    await self.ws.close()
                except:
                    pass
                self.ws = None
        # 监听任务退出时清理引用
        finally:
            self._monitor_task = None

    def to_tts(self, text: str) -> list:
        """非流式TTS处理，用于测试及保存音频文件的场景"""
        try:
            # 创建新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # 存储音频数据
            audio_data = []

            async def _generate_audio():
                # 建立WebSocket连接
                ws = await websockets.connect(
                    self.ws_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10,
                )
                try:
                    # 发送StartSynthesis请求
                    start_request = {
                        "header": {
                            "message_id": uuid.uuid4().hex,
                            "task_id": self.task_id,
                            "namespace": "FlowingSpeechSynthesizer",
                            "name": "StartSynthesis",
                            "appkey": self.appkey,
                        },
                        "payload": {
                            "voice": self.voice,
                            "format": self.format,
                            "sample_rate": self.sample_rate,
                            "volume": self.volume,
                            "speech_rate": self.speech_rate,
                            "pitch_rate": self.pitch_rate,
                            "enable_subtitle": True,
                        },
                    }
                    await ws.send(json.dumps(start_request))

                    # 等待SynthesisStarted响应
                    synthesis_started = False
                    while not synthesis_started:
                        msg = await ws.recv()
                        if isinstance(msg, str):
                            data = json.loads(msg)
                            header = data.get("header", {})
                            if header.get("name") == "SynthesisStarted":
                                synthesis_started = True
                                logger.bind(tag=TAG).debug("TTS合成已启动")
                            elif header.get("name") == "TaskFailed":
                                error_info = data.get("payload", {}).get(
                                    "error_info", {}
                                )
                                error_code = error_info.get("error_code")
                                error_message = error_info.get(
                                    "error_message", "未知错误"
                                )
                                raise Exception(
                                    f"启动合成失败: {error_code} - {error_message}"
                                )

                    # 发送文本合成请求
                    filtered_text = MarkdownCleaner.clean_markdown(text)
                    run_request = {
                        "header": {
                            "message_id": uuid.uuid4().hex,
                            "task_id": self.task_id,
                            "namespace": "FlowingSpeechSynthesizer",
                            "name": "RunSynthesis",
                            "appkey": self.appkey,
                        },
                        "payload": {"text": filtered_text},
                    }
                    await ws.send(json.dumps(run_request))

                    # 发送停止合成请求
                    stop_request = {
                        "header": {
                            "message_id": uuid.uuid4().hex,
                            "task_id": self.task_id,
                            "namespace": "FlowingSpeechSynthesizer",
                            "name": "StopSynthesis",
                            "appkey": self.appkey,
                        }
                    }
                    await ws.send(json.dumps(stop_request))

                    # 接收音频数据
                    synthesis_completed = False
                    while not synthesis_completed:
                        msg = await ws.recv()
                        if isinstance(msg, (bytes, bytearray)):
                            self.opus_encoder.encode_pcm_to_opus_stream(
                                msg,
                                end_of_stream=False,
                                callback=lambda opus: audio_data.append(opus)
                            )
                        elif isinstance(msg, str):
                            data = json.loads(msg)
                            header = data.get("header", {})
                            event_name = header.get("name")
                            if event_name == "SynthesisCompleted":
                                synthesis_completed = True
                                logger.bind(tag=TAG).debug("TTS合成完成")
                            elif event_name == "TaskFailed":
                                error_info = data.get("payload", {}).get(
                                    "error_info", {}
                                )
                                error_code = error_info.get("error_code")
                                error_message = error_info.get(
                                    "error_message", "未知错误"
                                )
                                raise Exception(
                                    f"合成失败: {error_code} - {error_message}"
                                )
                finally:
                    try:
                        await ws.close()
                    except:
                        pass

            loop.run_until_complete(_generate_audio())
            loop.close()

            return audio_data
        except Exception as e:
            logger.bind(tag=TAG).error(f"生成音频数据失败: {str(e)}")
            return []
