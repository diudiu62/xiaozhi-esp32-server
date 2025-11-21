import asyncio
import json
import logging

import aiohttp
import websockets


class TTSWebSocketServer:
    def __init__(self, host="0.0.0.0", port=8085):
        self.host = host
        self.port = port
        self.sessions = {}
        self.fish_url = "http://10.10.6.111:50000"

    async def handler(self, websocket):
        async for message in websocket:
            await self.process_message(websocket, message)

    async def process_message(self, websocket, message):
        data = json.loads(message)

        action = data.get("header", {}).get("action")
        task_id = data.get("header", {}).get("task_id")

        logging.info(f"2222: {action}")

        if action == "run-task":
            await self.start_task(websocket, task_id, data)
        elif action == "continue-task":
            await self.continue_task(websocket, task_id, data)
        elif action == "finish-task":
            await self.finish_task(websocket, task_id)

    async def start_task(self, websocket, task_id, data):
        session = {
            "task_id": task_id,
            "params": data.get("payload", {}).get("parameters", {}),
            "text": "",
        }
        user_id = session['params'].get('user', '')
        voice_id = session['params'].get('voice', '')
        sample_rate = session['params'].get('sample_rate', 0)
        # res = db.getVoice(user_id, voice_id)
        # if not res:
        #     logging.error(f"Voice not found: {voice_id}")
        #     return
        session['voice_id'] = f'{user_id}/{voice_id}'
        session['sample_rate'] = sample_rate
        self.sessions[task_id] = session

        logging.info(f"Started session: {session}")

        # 发送 task-started 事件
        await websocket.send(json.dumps({
            "header": {"event": "task-started", "task_id": task_id}
        }))

    async def continue_task(self, websocket, task_id, data):
        session = self.sessions.get(task_id)
        if not session:
            logging.warning(f"Session not found: {task_id}")
            return

        # 添加文本
        text = data.get("payload", {}).get("input", {}).get("text", "")
        session["text"] += text

        # 生成音频数据
        data = {
            "text": text,
            "reference_id": session['voice_id'],
            "streaming": True,
            "use_memory_cache": "on",
        }
        logging.info("TTS Request Data:", data)
        # pydantic_data = ServeTTSRequest(**data)
        # data = ormsgpack.packb(
        #     pydantic_data, option=ormsgpack.OPT_SERIALIZE_PYDANTIC)
        # headers = {'Content-Type': 'application/msgpack'}

        # async for chunk in self.fetch_audio(data, headers, sample_rate=session['sample_rate']):
        #     try:
        #         await websocket.send(chunk)
        #     except websockets.exceptions.ConnectionClosedError as e:
        #         logging.error(f"Connection closed: {e}")
        #         break
        # await websocket.send(json.dumps({
        #     "header": {"event": "sentence_end", "task_id": task_id, "text": text}
        # }))

    async def fetch_audio(self, data, headers, sample_rate=24000):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.fish_url}/v1/tts", data=data, headers=headers) as response:
                if response.status == 200:
                    async for chunk in response.content.iter_chunked(16000):
                        if chunk:
                            yield chunk
                else:
                    logging.error(f"Error fetching audio: {response.status}")

    async def finish_task(self, websocket, task_id):
        session = self.sessions.get(task_id)
        if not session:
            logging.warning(f"Session not found: {task_id}")
            return

        # 发送 task-finished 事件
        await websocket.send(json.dumps({
            "header": {"event": "task-finished", "task_id": task_id}
        }))

        # 清理会话
        del self.sessions[task_id]

    async def start_server(self):
        async with websockets.serve(
            self.handler, self.host, self.port,
            ping_interval=60,  # 每 30 秒发送一次 Ping 帧
            ping_timeout=30  # 等待 Pong 响应的超时时间为 10 秒
        ):
            logging.info(
                f"WebSocket TTS Server running on ws://{self.host}:{self.port}")
            await asyncio.Future()  # 阻塞以保持服务运行


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = TTSWebSocketServer(host="0.0.0.0", port=8085)
    asyncio.run(server.start_server())
