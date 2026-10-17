# ═══════════════════════════════════════════════════════════════
# main_web.py — ALL-IN-ONE SINGLE RUNNABLE FULL SYSTEM
# ═══════════════════════════════════════════════════════════════

import asyncio
import os
import json
import jwt
import aiosqlite
from datetime import datetime, timedelta
from typing import Dict, Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

# Auto-import your existing bot modules
import login_engine
import bot_core
from bot_core import (
    TcPOnLine, TcPChaT,
    build_ob55_online_auth, create_auth_token_chat
)

SECRET_KEY = "SUPER_SECRET_TCP_MASTER_KEY_#998"
ALGORITHM = "HS256"
ADMIN_PASSWORD = "frexyxtcp"
DB_FILE = "database.db"

app = FastAPI(title="Frexyx TCP Web Core")

# ═══════════════════════════════════════════════════════════════
# MULTI-INSTANCE BOT DAEMON & LOG BROADCASTER
# ═══════════════════════════════════════════════════════════════
class BotInstance:
    def __init__(self, user_id: int, uid: str, password: str, region: str):
        self.user_id = user_id
        self.uid = uid
        self.password = password
        self.region = region
        self.task: Optional[asyncio.Task] = None
        self.is_running = False
        self.logs = []
        self.websockets: list[WebSocket] = []

    async def emit_log(self, text: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_line = f"[{timestamp}] {text}"
        self.logs.append(log_line)
        if len(self.logs) > 200:
            self.logs.pop(0)
        
        # Broadcast immediately to connected browsers
        for ws in self.websockets:
            try:
                await ws.send_text(log_line)
            except Exception:
                pass

    async def run(self):
        self.is_running = True
        await self.emit_log(f"[*] Initializing TCP Bot Core | UID: {self.uid} | Region: {self.region}")
        while self.is_running:
            try:
                await self.emit_log("[*] Authenticating with Game Gateway...")
                result = await login_engine.do_login(self.uid, self.password, self.region)
                
                if not result:
                    await self.emit_log("[-] Login rejected by server. Retrying in 12 seconds...")
                    await asyncio.sleep(12)
                    continue

                acc_name = result.get("account_name", "Unknown")
                await self.emit_log(f"[+] Login SUCCESS! Name: {acc_name} | UID: {result['bot_uid']}")

                key = result["key"]
                iv = result["iv"]
                timestamp = result["timestamp"]
                online_ip = result["online_ip"]
                online_port = result["online_port"]
                chat_ip = result["chat_ip"]
                chat_port = result["chat_port"]

                ChatAutH = create_auth_token_chat(int(result["bot_uid"]), result["token"], int(timestamp), key, iv)
                OnlineAutH, _ = build_ob55_online_auth(int(result["bot_uid"]), result["token"], int(timestamp), key, iv)

                ready_event = asyncio.Event()
                await self.emit_log("[*] Hooking into TCP Chat Socket...")
                t1 = asyncio.create_task(
                    TcPChaT(chat_ip, chat_port, ChatAutH, key, iv, ready_event, result["region"])
                )
                await ready_event.wait()
                await self.emit_log("[+] TCP Chat connection verified!")
                await asyncio.sleep(1)

                await self.emit_log("[*] Hooking into TCP Online Socket...")
                t2 = asyncio.create_task(
                    TcPOnLine(online_ip, online_port, key, iv, OnlineAutH)
                )

                await self.emit_log("[🔥] BOT IS LIVE & ONLINE! Maintaining persistent session.")
                # Run for 7 hours, then cycle token
                await asyncio.wait_for(asyncio.gather(t1, t2), timeout=7 * 3600)

            except asyncio.CancelledError:
                await self.emit_log("[-] Manual termination signal received. Bot offline.")
                break
            except asyncio.TimeoutError:
                await self.emit_log("[!] 7-Hour session expired. Seamlessly recycling token...")
            except Exception as e:
                await self.emit_log(f"[!] System Exception: {e} -> Auto recovering in 7s...")
                await asyncio.sleep(7)

        self.is_running = False

    def stop(self):
        self.is_running = False
        if self.task and not self.task.done():
            self.task.cancel()


class EngineManager:
    def __init__(self):
        self.bots: Dict[int, BotInstance] = {}

    def get_bot(self, user_id: int) -> Optional[BotInstance]:
        return self.bots.get(user_id)

    async def start_bot(self, user_id: int, uid: str, password: str, region: str):
        if user_id in self.bots and self.bots[user_id].is_running:
            return False, "You already have an active bot running!"
        
        bot = BotInstance(user_id, uid, password, region)
        self.bots[user_id] = bot
        bot.task = asyncio.create_task(bot.run())
        return True, "Bot initialized successfully!"

    def stop_bot(self, user_id: int):
        bot = self.bots.get(user_id)
        if bot and bot.is_running:
            bot.stop()
            return True, "Bot session terminated!"
        return False, "No active bot found."

    async def restart_bot(self, user_id: int, uid: str, password: str, region: str):
        self.stop_bot(user_id)
        await asyncio.sleep(1.2)
        return await self.start_bot(user_id, uid, password, region)

engine_manager = EngineManager()

# ═══════════════════════════════════════════════════════════════
# DATABASE & STARTUP
# ═══════════════════════════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                username TEXT,
                password TEXT,
                avatar_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            INSERT OR IGNORE INTO settings (key, value) 
            VALUES ('splash_image', 'https://images.unsplash.com/photo-1618005182384-a83a8bd57fbe?q=80&w=1200&auto=format&fit=crop')
        """)
        await db.commit()

@app.on_event("startup")
async def on_startup():
    await init_db()

def create_token(user_id: int, email: str):
    payload = {"sub": user_id, "email": email, "exp": datetime.utcnow() + timedelta(days=7)}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(request: Request):
    token = request.cookies.get("auth_token")
    if not token:
        return None
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = data.get("sub")
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
            user = await cursor.fetchone()
            return dict(user) if user else None
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════
# EMBEDDED CYBER-BLUE & 3D RED HACKING TEXT INTERFACE
# ═══════════════════════════════════════════════════════════════
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TCP CLOUD MATRIX - 3D CORE</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>

    <style>
        @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;900&family=Rajdhani:wght@500;600;700&display=swap');
        * { font-family: 'Rajdhani', sans-serif; }
        .font-cyber { font-family: 'Orbitron', monospace; }

        /* Cyber Blue Color Grading */
        .glass-panel {
            background: rgba(3, 11, 28, 0.75);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid rgba(0, 140, 255, 0.28);
            box-shadow: 0 0 35px rgba(0, 100, 255, 0.18);
        }
        .blue-glow {
            border: 1px solid #0088ff;
            box-shadow: 0 0 25px rgba(0, 136, 255, 0.45);
        }
        .terminal-scroll::-webkit-scrollbar { width: 5px; }
        .terminal-scroll::-webkit-scrollbar-track { background: #01040a; }
        .terminal-scroll::-webkit-scrollbar-thumb { background: #0077ff; border-radius: 4px; }

        #canvas3d {
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            z-index: 0;
            pointer-events: none;
        }

        #splashScreen { perspective: 1200px; }
        .splash-card-3d {
            transform-style: preserve-3d;
            animation: float3D 4s ease-in-out infinite alternate;
        }
        @keyframes float3D {
            0% { transform: rotateY(-12deg) rotateX(7deg) translateY(-8px); }
            100% { transform: rotateY(12deg) rotateX(-7deg) translateY(8px); }
        }
    </style>
</head>
<body class="bg-[#010613] text-slate-100 min-h-screen overflow-x-hidden selection:bg-blue-600 selection:text-white">

    <!-- 3D RED HACKING TEXT THREE.JS CANVAS -->
    <canvas id="canvas3d"></canvas>

    <!-- 3D INTRO SPLASH -->
    <div id="splashScreen" class="fixed inset-0 z-50 flex flex-col items-center justify-center bg-[#01040d]/95 transition-opacity duration-700">
        <div class="relative splash-card-3d max-w-md w-11/12 p-4 glass-panel rounded-2xl blue-glow text-center">
            <div class="overflow-hidden rounded-xl h-60 border border-blue-500/40 relative">
                <img id="splashImg" src="__SPLASH_IMG__" class="w-full h-full object-cover">
                <div class="absolute inset-0 bg-gradient-to-t from-[#010613] via-transparent to-transparent"></div>
            </div>
            <h1 class="font-cyber text-2xl font-black mt-4 text-blue-400 tracking-wider">FREXYX <span class="text-[#ff003c]">DAEMON</span></h1>
            <p class="text-xs text-blue-300/70 mt-1 uppercase tracking-widest">3D Real-Time TCP Injection Engine</p>
            <button onclick="enterSystem()" class="mt-5 px-8 py-2.5 bg-gradient-to-r from-blue-600 via-indigo-600 to-[#ff003c] text-white font-cyber font-bold rounded-xl tracking-wider hover:opacity-90 shadow-lg shadow-blue-500/30 active:scale-95 transition">
                INITIALIZE MATRIX
            </button>
        </div>
    </div>

    <!-- NAVBAR -->
    <nav class="relative z-30 border-b border-blue-900/40 bg-[#030919]/80 backdrop-blur px-6 py-3.5 flex justify-between items-center">
        <div class="flex items-center gap-3">
            <div class="h-10 w-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-700 flex items-center justify-center text-white font-cyber font-extrabold shadow-lg shadow-blue-500/30">
                TCP
            </div>
            <div>
                <div class="font-cyber font-bold text-lg text-blue-400 tracking-wider">FREXYX <span class="text-white">TCP</span></div>
                <div class="text-[10px] text-blue-300/60 uppercase">Cluster Protocol Manager</div>
            </div>
        </div>

        <div class="flex items-center gap-4">
            <button onclick="openAdminModal()" class="text-xs font-cyber text-blue-300 hover:text-[#ff003c] border border-blue-800/80 hover:border-[#ff003c]/60 px-3 py-1.5 rounded-lg transition">
                /ADMIN
            </button>
            __USER_HEADER__
        </div>
    </nav>

    <!-- MAIN BODY -->
    <main class="relative z-20 max-w-7xl mx-auto px-4 py-8">
        __MAIN_CONTENT__
    </main>

    <!-- AUTH MODAL -->
    <div id="authModal" class="fixed inset-0 bg-black/85 backdrop-blur-md z-50 hidden flex items-center justify-center p-4">
        <div class="glass-panel w-full max-w-md p-6 rounded-2xl border-blue-500/40 relative">
            <button onclick="closeAuthModal()" class="absolute top-4 right-4 text-gray-400 hover:text-white"><i data-lucide="x" class="w-5 h-5"></i></button>
            <h3 class="font-cyber font-bold text-xl text-blue-400 text-center mb-6">SECURE ACCESS</h3>

            <button onclick="loginWithGoogleDemo()" class="w-full mb-4 py-2.5 bg-white text-black font-bold rounded-xl flex items-center justify-center gap-2 hover:bg-gray-200 transition">
                <img src="https://www.svgrepo.com/show/475656/google-color.svg" class="w-4 h-4">
                Continue with Google
            </button>

            <div class="flex items-center my-4">
                <div class="flex-grow border-t border-blue-900/40"></div>
                <span class="px-3 text-xs text-blue-400/60">OR CREDENTIALS</span>
                <div class="flex-grow border-t border-blue-900/40"></div>
            </div>

            <div class="space-y-3">
                <input id="authUsername" type="text" placeholder="Username (For Registration)" class="w-full bg-[#010613]/90 border border-blue-900/60 rounded-xl px-4 py-2 text-sm focus:border-blue-400 focus:outline-none text-white">
                <input id="authEmail" type="email" placeholder="Email Address" class="w-full bg-[#010613]/90 border border-blue-900/60 rounded-xl px-4 py-2 text-sm focus:border-blue-400 focus:outline-none text-white">
                <input id="authPassword" type="password" placeholder="Password" class="w-full bg-[#010613]/90 border border-blue-900/60 rounded-xl px-4 py-2 text-sm focus:border-blue-400 focus:outline-none text-white">
                
                <div class="flex gap-2 pt-2">
                    <button onclick="submitAuth('login')" class="flex-1 py-2.5 bg-blue-600 hover:bg-blue-500 text-white font-cyber font-bold rounded-xl text-xs transition">LOGIN</button>
                    <button onclick="submitAuth('register')" class="flex-1 py-2.5 bg-blue-950/60 border border-blue-800/60 hover:bg-blue-900/40 text-blue-300 font-cyber font-bold rounded-xl text-xs transition">REGISTER</button>
                </div>
            </div>
        </div>
    </div>

    <!-- ADMIN MODAL -->
    <div id="adminModal" class="fixed inset-0 bg-black/90 backdrop-blur-md z-50 hidden flex items-center justify-center p-4">
        <div class="glass-panel w-full max-w-4xl p-6 rounded-2xl border-[#ff003c]/30 relative max-h-[90vh] overflow-y-auto terminal-scroll">
            <button onclick="closeAdminModal()" class="absolute top-4 right-4 text-gray-400 hover:text-white"><i data-lucide="x" class="w-5 h-5"></i></button>
            <h3 class="font-cyber font-bold text-2xl text-blue-400 mb-2">CONTROL CORE : <span class="text-[#ff003c]">ADMIN PANEL</span></h3>
            
            <div id="adminPassStep" class="py-12 max-w-sm mx-auto text-center">
                <p class="text-xs text-blue-300/70 mb-4">Input master password for decryption</p>
                <input id="adminKeyInput" type="password" placeholder="Enter Password" class="w-full text-center bg-[#010613] border border-blue-900 rounded-xl px-4 py-2.5 mb-3 text-sm focus:border-[#ff003c] focus:outline-none text-white">
                <button onclick="verifyAdmin()" class="w-full py-2.5 bg-gradient-to-r from-blue-600 to-[#ff003c] text-white font-cyber font-bold rounded-xl text-xs hover:opacity-90">AUTHORIZE</button>
            </div>

            <div id="adminDataStep" class="hidden space-y-6">
                <div class="grid grid-cols-3 gap-4">
                    <div class="bg-[#010613]/80 border border-blue-900/50 p-4 rounded-xl text-center">
                        <div class="text-xs text-blue-300/70 uppercase">Registered Users</div>
                        <div id="admTotalUsers" class="text-2xl font-cyber font-bold text-blue-400 mt-1">0</div>
                    </div>
                    <div class="bg-[#010613]/80 border border-blue-900/50 p-4 rounded-xl text-center">
                        <div class="text-xs text-blue-300/70 uppercase">Bots Active (Online)</div>
                        <div id="admTotalRunning" class="text-2xl font-cyber font-bold text-cyan-400 mt-1">0</div>
                    </div>
                    <div class="bg-[#010613]/80 border border-blue-900/50 p-4 rounded-xl text-center">
                        <div class="text-xs text-blue-300/70 uppercase">Bots Offline</div>
                        <div id="admTotalStopped" class="text-2xl font-cyber font-bold text-[#ff003c] mt-1">0</div>
                    </div>
                </div>

                <div class="bg-[#010613]/80 p-4 rounded-xl border border-blue-900/50">
                    <label class="text-xs text-blue-300 font-cyber">3D INTRO SPLASH IMAGE URL</label>
                    <div class="flex gap-2 mt-2">
                        <input id="admSplashUrl" type="text" placeholder="https://..." class="flex-1 bg-black/90 border border-blue-900 rounded-xl px-4 py-2 text-sm focus:outline-none focus:border-blue-400 text-white">
                        <button onclick="updateSplashImage()" class="px-4 py-2 bg-blue-600 text-white font-bold font-cyber text-xs rounded-xl hover:bg-blue-500">UPDATE</button>
                    </div>
                </div>

                <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs border border-blue-900/40 rounded-xl">
                        <thead class="bg-[#010613] font-cyber text-blue-300 border-b border-blue-900/40">
                            <tr>
                                <th class="p-3">User</th>
                                <th class="p-3">Email</th>
                                <th class="p-3">Bot UID</th>
                                <th class="p-3">Status</th>
                            </tr>
                        </thead>
                        <tbody id="admUserTable" class="divide-y divide-blue-900/30 bg-[#010613]/40"></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <!-- ═══════════════════════════════════════════════════════════════ -->
    <!-- 3D RED HACKING TEXT ENGINE (THREE.JS WEBGL) -->
    <!-- ═══════════════════════════════════════════════════════════════ -->
    <script>
        const canvas = document.getElementById('canvas3d');
        const scene = new THREE.Scene();
        const camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 0.1, 1000);
        const renderer = new THREE.WebGLRenderer({ canvas: canvas, alpha: true, antialias: true });
        
        renderer.setSize(window.innerWidth, window.innerHeight);
        renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

        // 3D RED HACKING TEXT DICTIONARY
        const hackWords = [
            "0x7FF_INJECT", "TCP::DAEMON", "KERNEL_PANIC", "BYPASS_OB55",
            "HEX_OVERFLOW", "PACKET_STREAM", "AUTH_KEY_ACTIVE", "PORT_ROUTING",
            "TCP_ESTABLISHED", "MEMORY_CORRUPT", "BUFFER_RUN", "ROOT_ACCESS"
        ];
        const textObjects = [];

        function createRedTextTexture(text) {
            const tempCanvas = document.createElement('canvas');
            tempCanvas.width = 512;
            tempCanvas.height = 128;
            const ctx = tempCanvas.getContext('2d');

            ctx.clearRect(0, 0, tempCanvas.width, tempCanvas.height);
            ctx.font = '900 38px Orbitron, monospace';
            ctx.fillStyle = '#ff003c'; // Pure Laser Red
            ctx.textAlign = 'center';
            ctx.shadowColor = '#ff003c';
            ctx.shadowBlur = 20;
            ctx.fillText(text, 256, 75);

            return new THREE.CanvasTexture(tempCanvas);
        }

        for (let i = 0; i < 35; i++) {
            const word = hackWords[Math.floor(Math.random() * hackWords.length)];
            const texture = createRedTextTexture(word);
            const material = new THREE.SpriteMaterial({ map: texture, transparent: true, opacity: 0.7 });
            const sprite = new THREE.Sprite(material);

            sprite.position.x = (Math.random() - 0.5) * 85;
            sprite.position.y = (Math.random() - 0.5) * 65;
            sprite.position.z = (Math.random() - 0.5) * 70 - 15;

            sprite.scale.set(12, 3, 1);
            sprite.userData = {
                speedY: (Math.random() * 0.025) + 0.015
            };

            scene.add(sprite);
            textObjects.push(sprite);
        }

        // RED ENERGY PARTICLES
        const count = 180;
        const geom = new THREE.BufferGeometry();
        const coords = new Float32Array(count * 3);

        for (let i = 0; i < count * 3; i += 3) {
            coords[i] = (Math.random() - 0.5) * 110;
            coords[i + 1] = (Math.random() - 0.5) * 90;
            coords[i + 2] = (Math.random() - 0.5) * 80;
        }
        geom.setAttribute('position', new THREE.BufferAttribute(coords, 3));

        const pMat = new THREE.PointsMaterial({ color: 0xff003c, size: 0.6, transparent: true, opacity: 0.6 });
        const pSystem = new THREE.Points(geom, pMat);
        scene.add(pSystem);

        camera.position.z = 25;

        // Interactive Mouse Parallax
        let mouseX = 0, mouseY = 0;
        window.addEventListener('mousemove', (e) => {
            mouseX = (e.clientX - window.innerWidth / 2) * 0.0004;
            mouseY = (e.clientY - window.innerHeight / 2) * 0.0004;
        });

        function animate() {
            requestAnimationFrame(animate);

            textObjects.forEach(s => {
                s.position.y += s.userData.speedY;
                if (s.position.y > 35) s.position.y = -35;
            });

            pSystem.rotation.y += 0.0008;
            pSystem.rotation.x += 0.0005;

            camera.position.x += (mouseX * 15 - camera.position.x) * 0.05;
            camera.position.y += (-mouseY * 15 - camera.position.y) * 0.05;
            camera.lookAt(scene.position);

            renderer.render(scene, camera);
        }
        animate();

        window.addEventListener('resize', () => {
            camera.aspect = window.innerWidth / window.innerHeight;
            camera.updateProjectionMatrix();
            renderer.setSize(window.innerWidth, window.innerHeight);
        });
    </script>

    <!-- FRONTEND LOGIC -->
    <script>
        lucide.createIcons();
        let currentUserId = __USER_ID__;
        let ws = null;

        function enterSystem() {
            const splash = document.getElementById('splashScreen');
            splash.classList.add('opacity-0', 'pointer-events-none');
            setTimeout(() => splash.remove(), 700);
        }

        if (window.location.pathname === '/admin') {
            openAdminModal();
        }

        function initWebSocket() {
            if (!currentUserId) return;
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws/logs/${currentUserId}`);

            ws.onmessage = (event) => {
                const terminal = document.getElementById('terminalBody');
                const log = document.createElement('p');
                log.innerText = event.data;
                terminal.appendChild(log);
                terminal.scrollTop = terminal.scrollHeight;
            };
            ws.onclose = () => { setTimeout(initWebSocket, 4000); };
        }
        if (currentUserId) {
            initWebSocket();
            fetchBotStatus();
        }

        async function fetchBotStatus() {
            const res = await fetch('/api/bot/status');
            const data = await res.json();
            const badge = document.getElementById('botStatusBadge');
            if (data.online) {
                badge.innerText = "ONLINE";
                badge.className = "px-2.5 py-0.5 rounded-full text-[11px] font-cyber bg-blue-500/20 text-blue-400 border border-blue-500/40";
                if (data.uid) document.getElementById('inputUid').value = data.uid;
            } else {
                badge.innerText = "OFFLINE";
                badge.className = "px-2.5 py-0.5 rounded-full text-[11px] font-cyber bg-[#ff003c]/20 text-[#ff003c] border border-[#ff003c]/30";
            }
        }

        async function controlBot(action) {
            const uid = document.getElementById('inputUid').value;
            const pw = document.getElementById('inputPw').value;
            const region = document.getElementById('inputRegion').value;

            const res = await fetch('/api/bot/action', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action, uid, pw, region })
            });
            const data = await res.json();
            alert(data.message);
            fetchBotStatus();
        }

        function clearTerminal() { document.getElementById('terminalBody').innerHTML = ''; }
        function showAuthModal() { document.getElementById('authModal').classList.remove('hidden'); }
        function closeAuthModal() { document.getElementById('authModal').classList.add('hidden'); }

        async function submitAuth(type) {
            const email = document.getElementById('authEmail').value;
            const password = document.getElementById('authPassword').value;
            const username = document.getElementById('authUsername').value;

            const endpoint = type === 'login' ? '/api/auth/login' : '/api/auth/register';
            const res = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email, password, username })
            });
            const data = await res.json();
            alert(data.message);
            if (data.status && type === 'login') window.location.reload();
        }

        async function loginWithGoogleDemo() {
            const email = prompt("Enter your Google Account Email:");
            if (!email) return;
            const name = email.split('@')[0];
            const picture = `https://api.dicebear.com/7.x/bottts/svg?seed=${name}`;

            const res = await fetch('/api/auth/google', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email, name, picture })
            });
            const data = await res.json();
            if (data.status) window.location.reload();
        }

        async function changeAvatarPrompt() {
            const newUrl = prompt("Enter new Profile Image URL:");
            if (!newUrl) return;
            const res = await fetch('/api/user/update-avatar', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ avatar_url: newUrl })
            });
            const data = await res.json();
            if (data.status) {
                document.getElementById('userAvatarImg').src = newUrl;
                alert("Avatar Updated Successfully!");
            }
        }

        async function logout() {
            await fetch('/api/auth/logout');
            window.location.reload();
        }

        function openAdminModal() { document.getElementById('adminModal').classList.remove('hidden'); }
        function closeAdminModal() { document.getElementById('adminModal').classList.add('hidden'); }

        async function verifyAdmin() {
            const pass = document.getElementById('adminKeyInput').value;
            const res = await fetch('/api/admin/verify', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ password: pass })
            });
            const data = await res.json();
            if (data.status) {
                document.getElementById('adminPassStep').classList.add('hidden');
                document.getElementById('adminDataStep').classList.remove('hidden');
                loadAdminData(pass);
            } else {
                alert("ACCESS DENIED: Master password incorrect!");
            }
        }

        async function loadAdminData(pass) {
            const res = await fetch(`/api/admin/overview?password=${pass}`);
            const data = await res.json();
            document.getElementById('admTotalUsers').innerText = data.total_users;
            document.getElementById('admTotalRunning').innerText = data.total_running;
            document.getElementById('admTotalStopped').innerText = data.total_stopped;

            const tbody = document.getElementById('admUserTable');
            tbody.innerHTML = '';
            data.users.forEach(u => {
                const bot = data.bots.find(b => b.user_id === u.id);
                const isRunning = bot && bot.is_running;
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td class="p-3 flex items-center gap-2">
                        <img src="${u.avatar_url}" class="w-6 h-6 rounded-full border border-blue-500">
                        <span>${u.username}</span>
                    </td>
                    <td class="p-3 text-blue-200/60">${u.email}</td>
                    <td class="p-3 font-mono text-cyan-400">${bot ? bot.uid : 'N/A'}</td>
                    <td class="p-3">
                        <span class="px-2 py-0.5 rounded text-[10px] font-cyber ${isRunning ? 'bg-blue-500/20 text-blue-300 border border-blue-500/40' : 'bg-[#ff003c]/20 text-[#ff003c] border border-[#ff003c]/30'}">
                            ${isRunning ? 'ONLINE' : 'STOPPED'}
                        </span>
                    </td>
                `;
                tbody.appendChild(tr);
            });
        }

        async function updateSplashImage() {
            const splashUrl = document.getElementById('admSplashUrl').value;
            const pass = document.getElementById('adminKeyInput').value;
            const res = await fetch('/api/admin/set-splash', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ admin_password: pass, splash_image: splashUrl })
            });
            const data = await res.json();
            alert(data.message);
        }
    </script>
</body>
</html>
"""

# ═══════════════════════════════════════════════════════════════
# APP ROUTES
# ═══════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def serve_home(request: Request):
    user = await get_current_user(request)
    
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = 'splash_image'")
        row = await cur.fetchone()
        splash_image = row[0] if row else ""

    if user:
        user_header = f"""
        <div class="flex items-center gap-3 glass-panel px-3 py-1.5 rounded-xl border-blue-500/40">
            <img id="userAvatarImg" src="{user['avatar_url']}" onclick="changeAvatarPrompt()" title="Click to Change Avatar" class="h-8 w-8 rounded-full ring-2 ring-blue-400 cursor-pointer object-cover">
            <div class="hidden sm:block text-left">
                <p class="text-xs font-bold text-blue-300">{user['username']}</p>
                <p class="text-[10px] text-blue-200/50 truncate max-w-[120px]">{user['email']}</p>
            </div>
            <button onclick="logout()" class="text-gray-400 hover:text-[#ff003c] ml-2"><i data-lucide="log-out" class="w-4 h-4"></i></button>
        </div>
        """
        main_content = """
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div class="glass-panel p-6 rounded-2xl relative">
                <div class="flex items-center justify-between pb-4 border-b border-blue-900/40">
                    <h2 class="font-cyber font-bold text-base text-blue-300 flex items-center gap-2">
                        <i data-lucide="cpu" class="w-5 h-5 text-blue-400"></i> BOT CONTROLLER
                    </h2>
                    <span id="botStatusBadge" class="px-2.5 py-0.5 rounded-full text-[11px] font-cyber bg-[#ff003c]/20 text-[#ff003c] border border-[#ff003c]/30">
                        OFFLINE
                    </span>
                </div>

                <div class="space-y-4 mt-5">
                    <div>
                        <label class="text-xs text-blue-300/80 uppercase">Guest UID</label>
                        <input id="inputUid" type="text" placeholder="e.g. 8203676727" class="w-full mt-1 bg-[#010613]/90 border border-blue-900/60 rounded-xl px-4 py-2.5 text-sm focus:border-blue-400 focus:outline-none transition text-white">
                    </div>
                    <div>
                        <label class="text-xs text-blue-300/80 uppercase">Guest Password / Hash</label>
                        <input id="inputPw" type="password" placeholder="Hex SHA256 Password" class="w-full mt-1 bg-[#010613]/90 border border-blue-900/60 rounded-xl px-4 py-2.5 text-sm focus:border-blue-400 focus:outline-none transition text-white">
                    </div>
                    <div>
                        <label class="text-xs text-blue-300/80 uppercase">Region Target</label>
                        <select id="inputRegion" class="w-full mt-1 bg-[#010613]/90 border border-blue-900/60 rounded-xl px-4 py-2.5 text-sm focus:border-blue-400 focus:outline-none text-white">
                            <option value="BD">BD (Bangladesh)</option>
                            <option value="IND">IND (India)</option>
                            <option value="SG">SG (Singapore)</option>
                            <option value="PK">PK (Pakistan)</option>
                            <option value="ID">ID (Indonesia)</option>
                            <option value="ME">ME (Middle East)</option>
                        </select>
                    </div>

                    <div class="grid grid-cols-3 gap-2 pt-4">
                        <button onclick="controlBot('start')" class="py-2.5 bg-blue-600 hover:bg-blue-500 text-white font-cyber font-bold rounded-xl text-xs flex items-center justify-center gap-1 shadow-lg shadow-blue-500/20 active:scale-95 transition">
                            <i data-lucide="play" class="w-3.5 h-3.5"></i> START
                        </button>
                        <button onclick="controlBot('stop')" class="py-2.5 bg-[#ff003c]/20 hover:bg-[#ff003c] text-[#ff003c] hover:text-white border border-[#ff003c]/40 font-cyber font-bold rounded-xl text-xs flex items-center justify-center gap-1 active:scale-95 transition">
                            <i data-lucide="square" class="w-3.5 h-3.5"></i> STOP
                        </button>
                        <button onclick="controlBot('restart')" class="py-2.5 bg-indigo-600/20 hover:bg-indigo-600 text-indigo-300 hover:text-white border border-indigo-500/40 font-cyber font-bold rounded-xl text-xs flex items-center justify-center gap-1 active:scale-95 transition">
                            <i data-lucide="rotate-cw" class="w-3.5 h-3.5"></i> REBOOT
                        </button>
                    </div>
                </div>
            </div>

            <div class="lg:col-span-2 glass-panel rounded-2xl flex flex-col h-[520px] overflow-hidden border border-blue-900/50">
                <div class="bg-[#030919]/90 px-4 py-3 flex justify-between items-center border-b border-blue-900/40">
                    <div class="flex items-center gap-2">
                        <span class="w-3 h-3 rounded-full bg-[#ff003c] inline-block"></span>
                        <span class="w-3 h-3 rounded-full bg-yellow-500 inline-block"></span>
                        <span class="w-3 h-3 rounded-full bg-blue-500 inline-block"></span>
                        <span class="text-xs font-cyber text-blue-300/80 ml-2">TELEMETRY CONSOLE STREAM</span>
                    </div>
                    <button onclick="clearTerminal()" class="text-xs text-blue-400 hover:text-[#ff003c]"><i data-lucide="trash-2" class="w-4 h-4"></i></button>
                </div>
                <div id="terminalBody" class="flex-1 p-4 font-mono text-xs text-cyan-400 bg-[#01040d]/95 overflow-y-auto terminal-scroll space-y-1">
                    <p class="text-gray-500">[System] Realtime WebSocket Ready. Press Start to launch Bot.</p>
                </div>
            </div>
        </div>
        """
        user_id_str = str(user["id"])
    else:
        user_header = """
        <button onclick="showAuthModal()" class="px-5 py-2 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white font-cyber font-bold rounded-xl text-xs shadow-lg shadow-blue-600/30 transition">
            PORTAL LOGIN
        </button>
        """
        main_content = """
        <div class="text-center py-24">
            <div class="inline-block px-4 py-1.5 rounded-full border border-blue-500/30 bg-blue-950/40 text-blue-300 text-xs font-cyber mb-4">
                [SYSTEM READY] PROTOCOL OB55 STABLE
            </div>
            <h1 class="font-cyber text-4xl sm:text-6xl font-black text-transparent bg-clip-text bg-gradient-to-r from-blue-400 via-cyan-300 to-indigo-400 tracking-wide">
                MULTI-USER TCP CLOUD ENGINE
            </h1>
            <p class="mt-4 text-blue-200/60 text-base sm:text-lg max-w-2xl mx-auto">
                Host your FreeFire TCP guest bot 24/7 with realtime logs reflection, automated reconnection, and 3D telemetry monitoring.
            </p>
            <div class="mt-8 flex justify-center gap-4">
                <button onclick="showAuthModal()" class="px-8 py-3 bg-gradient-to-r from-blue-600 to-[#ff003c] text-white font-cyber font-bold rounded-xl text-sm shadow-xl shadow-blue-600/30 hover:scale-105 transition">
                    INITIALIZE CLIENT
                </button>
            </div>
        </div>
        """
        user_id_str = "null"

    html = (
        HTML_TEMPLATE.replace("__SPLASH_IMG__", splash_image)
        .replace("__USER_HEADER__", user_header)
        .replace("__MAIN_CONTENT__", main_content)
        .replace("__USER_ID__", user_id_str)
    )
    return HTMLResponse(html)

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    return await serve_home(request)

# ═══════════════════════════════════════════════════════════════
# AUTH & BOT CONTROLLER APIS
# ═══════════════════════════════════════════════════════════════
@app.post("/api/auth/register")
async def register(data: dict):
    email = data.get("email")
    username = data.get("username")
    password = data.get("password")
    avatar = f"https://api.dicebear.com/7.x/bottts/svg?seed={username}"

    async with aiosqlite.connect(DB_FILE) as db:
        try:
            await db.execute(
                "INSERT INTO users (email, username, password, avatar_url) VALUES (?, ?, ?, ?)",
                (email, username, password, avatar)
            )
            await db.commit()
            return {"status": True, "message": "Registered! You can now login."}
        except aiosqlite.IntegrityError:
            return JSONResponse({"status": False, "message": "Email already exists!"}, status_code=400)

@app.post("/api/auth/login")
async def login(data: dict):
    email = data.get("email")
    password = data.get("password")
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE email = ? AND password = ?", (email, password))
        user = await cur.fetchone()
        if not user:
            return JSONResponse({"status": False, "message": "Invalid email or password!"}, status_code=401)
        
        token = create_token(user["id"], user["email"])
        res = JSONResponse({"status": True, "message": "Login success!"})
        res.set_cookie(key="auth_token", value=token, httponly=True, max_age=7*86400)
        return res

@app.post("/api/auth/google")
async def google_login(data: dict):
    email = data.get("email")
    name = data.get("name")
    picture = data.get("picture")

    if not email:
        return JSONResponse({"status": False, "message": "Invalid Google details"}, status_code=400)

    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE email = ?", (email,))
        user = await cur.fetchone()
        
        if not user:
            cursor = await db.execute(
                "INSERT INTO users (email, username, password, avatar_url) VALUES (?, ?, '', ?)",
                (email, name, picture)
            )
            await db.commit()
            user_id = cursor.lastrowid
        else:
            user_id = user["id"]
            if picture and user["avatar_url"] != picture:
                await db.execute("UPDATE users SET avatar_url = ? WHERE id = ?", (picture, user_id))
                await db.commit()

        token = create_token(user_id, email)
        res = JSONResponse({"status": True, "message": "Google Login Success!"})
        res.set_cookie(key="auth_token", value=token, httponly=True, max_age=7*86400)
        return res

@app.post("/api/user/update-avatar")
async def update_avatar(data: dict, request: Request):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"status": False, "message": "Unauthorized"}, status_code=401)
    new_avatar = data.get("avatar_url")
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET avatar_url = ? WHERE id = ?", (new_avatar, user["id"]))
        await db.commit()
    return {"status": True, "message": "Profile avatar updated!"}

@app.get("/api/auth/logout")
async def logout():
    res = JSONResponse({"status": True, "message": "Logged out"})
    res.delete_cookie("auth_token")
    return res

@app.post("/api/bot/action")
async def bot_action(data: dict, request: Request):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"status": False, "message": "Unauthorized"}, status_code=401)
    
    action = data.get("action")
    uid = data.get("uid", "").strip()
    pw = data.get("pw", "").strip()
    region = data.get("region", "BD").strip()

    if action == "start":
        if not uid or not pw:
            return {"status": False, "message": "UID and Password required!"}
        ok, msg = await engine_manager.start_bot(user["id"], uid, pw, region)
        return {"status": ok, "message": msg}

    elif action == "stop":
        ok, msg = engine_manager.stop_bot(user["id"])
        return {"status": ok, "message": msg}

    elif action == "restart":
        if not uid or not pw:
            return {"status": False, "message": "UID and Password required!"}
        ok, msg = await engine_manager.restart_bot(user["id"], uid, pw, region)
        return {"status": ok, "message": msg}

    return {"status": False, "message": "Invalid Action"}

@app.get("/api/bot/status")
async def bot_status(request: Request):
    user = await get_current_user(request)
    if not user:
        return {"online": False}
    bot = engine_manager.get_bot(user["id"])
    return {
        "online": bot.is_running if bot else False,
        "uid": bot.uid if bot else None,
        "region": bot.region if bot else None
    }

@app.websocket("/ws/logs/{user_id}")
async def ws_logs(websocket: WebSocket, user_id: int):
    await websocket.accept()
    bot = engine_manager.get_bot(user_id)
    if bot:
        bot.websockets.append(websocket)
        for old_log in bot.logs[-50:]:
            await websocket.send_text(old_log)
    else:
        await websocket.send_text("[SYSTEM] Daemon idle. Press START to launch bot.")

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if bot and websocket in bot.websockets:
            bot.websockets.remove(websocket)

# ═══════════════════════════════════════════════════════════════
# ADMIN PANEL ENDPOINTS
# ═══════════════════════════════════════════════════════════════
@app.post("/api/admin/verify")
async def verify_admin(data: dict):
    if data.get("password") == ADMIN_PASSWORD:
        return {"status": True}
    return JSONResponse({"status": False, "message": "Invalid Admin Password!"}, status_code=403)

@app.get("/api/admin/overview")
async def admin_overview(password: str):
    if password != ADMIN_PASSWORD:
        return JSONResponse({"status": False, "message": "Unauthorized"}, status_code=403)
    
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id, username, email, avatar_url, created_at FROM users")
        users = [dict(u) for u in await cur.fetchall()]

    active_bots = []
    total_running = 0
    for uid_key, bot in engine_manager.bots.items():
        if bot.is_running:
            total_running += 1
        active_bots.append({
            "user_id": bot.user_id,
            "uid": bot.uid,
            "region": bot.region,
            "is_running": bot.is_running
        })

    return {
        "total_users": len(users),
        "total_running": total_running,
        "total_stopped": len(users) - total_running,
        "users": users,
        "bots": active_bots
    }

@app.post("/api/admin/set-splash")
async def set_splash(data: dict):
    if data.get("admin_password") != ADMIN_PASSWORD:
        return JSONResponse({"status": False, "message": "Unauthorized"}, status_code=403)
    new_url = data.get("splash_image")
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('splash_image', ?)", (new_url,))
        await db.commit()
    return {"status": True, "message": "3D Intro Splash Image Updated!"}


if __name__ == '__main__':
    print("[*] Starting Frexyx TCP 3D Server at http://0.0.0.0:8000")
    uvicorn.run("main_web:app", host="0.0.0.0", port=8000, reload=True)