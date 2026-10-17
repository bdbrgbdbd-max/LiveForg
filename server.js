import express from 'express';
import http from 'node:http';
import os from 'node:os';
import { Server } from 'socket.io';
import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: false } });
const PORT = Number(process.env.PORT || 3000);
let liveConnection = null;
let activeUsername = '';
let connecting = false;

app.get('/', (_req, res) => res.sendFile(new URL('./index.html', import.meta.url).pathname));
app.get('/health', (_req, res) => res.json({ ok: true, app: 'LiveForge', connected: !!liveConnection?.isConnected, username: activeUsername || null }));

function status(state, details = {}) {
  io.emit('lf:connection-status', { state, username: activeUsername, ...details, at: new Date().toISOString() });
}
function sendEvent(type, data = {}) {
  const user = data.user || data.uniqueId || data.nickname || data.nickName || 'مشاهد';
  const payload = {
    type,
    user: String(user).replace(/^@/, ''),
    text: String(data.text || data.comment || data.description || data.giftName || data.label || type),
    count: Number(data.count || data.likeCount || data.repeatCount || 1),
    giftName: data.giftName || data.giftDetails?.giftName || null,
    giftImage: data.giftPictureUrl || data.giftDetails?.giftImageUrl || null,
    avatar: data.profilePictureUrl || data.user?.profilePicture?.url?.[0] || null,
    rawType: type,
    at: new Date().toISOString()
  };
  io.emit('lf:live-event', payload);
}
function attachEvents(conn) {
  const on = (event, type, formatter) => {
    try { conn.on(event, data => { try { sendEvent(type, formatter ? formatter(data || {}) : data || {}); } catch (e) { console.error('event mapping error', type, e); } }); }
    catch (e) { console.warn('Event not available in this connector build:', event, e?.message || e); }
  };
  on(WebcastEvent.CHAT, 'comment', d => ({ user: d.user?.uniqueId || d.user?.nickname || d.uniqueId, text: d.comment || '' }));
  on(WebcastEvent.GIFT, 'gift', d => ({ user: d.user?.uniqueId || d.user?.nickname || d.uniqueId, text: `${d.giftDetails?.giftName || d.giftName || 'هدية'}${d.repeatCount > 1 ? ` ×${d.repeatCount}` : ''}`, giftName: d.giftDetails?.giftName || d.giftName, count: d.repeatCount || 1, avatar: d.user?.profilePicture?.url?.[0] }));
  on(WebcastEvent.LIKE, 'like', d => ({ user: d.user?.uniqueId || d.user?.nickname || d.uniqueId, text: `إعجاب ×${d.likeCount || d.count || 1}`, count: d.likeCount || d.count || 1 }));
  on(WebcastEvent.MEMBER, 'join', d => ({ user: d.user?.uniqueId || d.user?.nickname || d.uniqueId, text: 'انضم إلى البث' }));
  on(WebcastEvent.ROOM_USER, 'viewers', d => ({ user: 'إحصائيات البث', text: `المشاهدون: ${d.viewerCount ?? d.total ?? 'تحديث'}`, count: d.viewerCount ?? d.total ?? 0 }));
  on(WebcastEvent.SUBSCRIBE, 'subscribe', d => ({ user: d.user?.uniqueId || d.user?.nickname || d.uniqueId, text: 'اشترك في البث' }));
  on(WebcastEvent.QUESTION_NEW, 'question', d => ({ user: d.user?.uniqueId || d.user?.nickname || d.uniqueId, text: d.questionText || d.text || 'سؤال جديد' }));
  on(WebcastEvent.SOCIAL, 'social', d => ({ user: d.user?.uniqueId || d.user?.nickname || d.uniqueId, text: d.displayType || d.label || 'تفاعل اجتماعي' }));
  on('follow', 'follow', d => ({ user: d.uniqueId || d.nickname, text: 'بدأ بمتابعتك' }));
  on('share', 'share', d => ({ user: d.uniqueId || d.nickname, text: 'شارك البث' }));
  conn.on('disconnected', () => { liveConnection = null; connecting = false; status('disconnected', { message: 'انقطع الاتصال بالبث' }); });
  conn.on('streamEnd', () => { status('stream-ended', { message: 'انتهى البث' }); });
  conn.on('error', err => { console.error('TikTok LIVE error:', err?.message || err); status('error', { message: String(err?.message || 'حدث خطأ في اتصال TikTok LIVE') }); });
}

io.on('connection', socket => {
  socket.emit('lf:connection-status', { state: liveConnection?.isConnected ? 'connected' : connecting ? 'connecting' : 'disconnected', username: activeUsername });
  socket.on('lf:connect-tiktok', async (input, ack = () => {}) => {
    const username = String(input || '').trim().replace(/^@/, '').replace(/[^\w.]/g, '');
    if (!username) return ack({ ok: false, message: 'اكتب اسم حساب TikTok بدون @ أو معها' });
    if (connecting) return ack({ ok: false, message: 'الاتصال جارٍ بالفعل، انتظر قليلاً' });
    connecting = true;
    activeUsername = username;
    status('connecting', { message: 'جارٍ الاتصال بالبث الحقيقي…' });
    try {
      if (liveConnection) {
        try { await liveConnection.disconnect(); } catch {}
        liveConnection = null;
      }
      const conn = new TikTokLiveConnection(username, {
  processInitialData: false
});
      const result = await conn.connect();
      liveConnection = conn;
      connecting = false;
      status('connected', { roomId: result?.roomId || conn.roomId || null, message: 'تم الاتصال بالبث الحقيقي' });
      ack({ ok: true, username, roomId: result?.roomId || conn.roomId || null });
    } catch (err) {
      connecting = false;
      liveConnection = null;
      const message = String(err?.message || 'تعذّر الاتصال. تأكد أن الحساب يبث حالياً وأن الاتصال بالإنترنت يعمل.');
      status('error', { message });
      ack({ ok: false, message });
    }
  });
  socket.on('lf:disconnect-tiktok', async () => {
    if (liveConnection) { try { await liveConnection.disconnect(); } catch {} }
    liveConnection = null; connecting = false; status('disconnected', { message: 'تم فصل الاتصال' });
  });
});

function localIPs() {
  const out = [];
  for (const list of Object.values(os.networkInterfaces())) for (const info of list || []) if (info.family === 'IPv4' && !info.internal) out.push(info.address);
  return [...new Set(out)];
}
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\nLiveForge running: http://localhost:${PORT}`);
  for (const ip of localIPs()) console.log(`Phone/tablet on same Wi-Fi: http://${ip}:${PORT}`);
  console.log('Press Ctrl+C to stop.');
});
