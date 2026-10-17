
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

app.get('/', (_req, res) => {
  res.sendFile(new URL('./index.html', import.meta.url).pathname);
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    app: 'LiveForge',
    connected: !!liveConnection?.isConnected,
    username: activeUsername || null
  });
});

function status(state, details = {}) {
  io.emit('lf:connection-status', {
    state,
    username: activeUsername,
    ...details,
    at: new Date().toISOString()
  });
}

function sendEvent(type, data = {}) {
  const userData = data.user;
  const user =
    (typeof userData === 'string' ? userData : null) ||
    userData?.uniqueId ||
    userData?.nickname ||
    data.uniqueId ||
    data.nickname ||
    data.nickName ||
    'مشاهد';

  const giftName =
    data.giftDetails?.giftName ||
    data.giftName ||
    null;

  const payload = {
    type,
    user: String(user).replace(/^@/, ''),
    text: String(
      data.text ||
      data.comment ||
      data.description ||
      giftName ||
      data.label ||
      type
    ),
    count: Number(
      data.count ||
      data.likeCount ||
      data.repeatCount ||
      1
    ),
    giftName,
    giftImage:
      data.giftPictureUrl ||
      data.giftDetails?.giftImageUrl ||
      null,
    avatar:
      data.profilePictureUrl ||
      userData?.profilePicture?.url?.[0] ||
      null,
    rawType: type,
    at: new Date().toISOString()
  };

  io.emit('lf:live-event', payload);
}

function attachEvents(conn) {
  const on = (event, type, formatter) => {
    if (!event) return;

    try {
      conn.on(event, data => {
        try {
          sendEvent(type, formatter ? formatter(data || {}) : data || {});
        } catch (error) {
          console.error('Event mapping error:', type, error);
        }
      });
    } catch (error) {
      console.warn(
        'Event registration warning:',
        event,
        error?.message || error
      );
    }
  };

  on(WebcastEvent.CHAT, 'comment', d => ({
    user: d.user?.uniqueId || d.user?.nickname || d.uniqueId,
    text: d.comment || ''
  }));

  on(WebcastEvent.GIFT, 'gift', d => ({
    user: d.user?.uniqueId || d.user?.nickname || d.uniqueId,
    text: `${d.giftDetails?.giftName || d.giftName || 'هدية'}${
      d.repeatCount > 1 ? ` ×${d.repeatCount}` : ''
    }`,
    giftName: d.giftDetails?.giftName || d.giftName,
    count: d.repeatCount || 1,
    avatar: d.user?.profilePicture?.url?.[0]
  }));

  on(WebcastEvent.LIKE, 'like', d => ({
    user: d.user?.uniqueId || d.user?.nickname || d.uniqueId,
    text: `إعجاب ×${d.likeCount || d.count || 1}`,
    count: d.likeCount || d.count || 1
  }));

  on(WebcastEvent.MEMBER, 'join', d => ({
    user: d.user?.uniqueId || d.user?.nickname || d.uniqueId,
    text: 'انضم إلى البث'
  }));

  on(WebcastEvent.ROOM_USER, 'viewers', d => ({
    user: 'إحصائيات البث',
    text: `المشاهدون: ${d.viewerCount ?? d.total ?? 'تحديث'}`,
    count: d.viewerCount ?? d.total ?? 0
  }));

  on(WebcastEvent.SUBSCRIBE, 'subscribe', d => ({
    user: d.user?.uniqueId || d.user?.nickname || d.uniqueId,
    text: 'اشترك في البث'
  }));

  on(WebcastEvent.QUESTION_NEW, 'question', d => ({
    user: d.user?.uniqueId || d.user?.nickname || d.uniqueId,
    text: d.questionText || d.text || 'سؤال جديد'
  }));

  on(WebcastEvent.SOCIAL, 'social', d => ({
    user: d.user?.uniqueId || d.user?.nickname || d.uniqueId,
    text: d.displayType || d.label || 'تفاعل اجتماعي'
  }));

  on('follow', 'follow', d => ({
    user: d.uniqueId || d.nickname,
    text: 'بدأ بمتابعتك'
  }));

  on('share', 'share', d => ({
    user: d.uniqueId || d.nickname,
    text: 'شارك البث'
  }));

  conn.on('disconnected', () => {
    if (liveConnection === conn) {
      liveConnection = null;
      connecting = false;
      status('disconnected', { message: 'انقطع الاتصال بالبث' });
    }
  });

  conn.on('streamEnd', () => {
    status('stream-ended', { message: 'انتهى البث' });
  });

  conn.on('error', error => {
    console.error('TikTok LIVE error:', error?.stack || error);
    status('error', {
      message: String(
        error?.message || 'حدث خطأ في اتصال TikTok LIVE'
      )
    });
  });
}

io.on('connection', socket => {
  socket.emit('lf:connection-status', {
    state: liveConnection?.isConnected
      ? 'connected'
      : connecting
        ? 'connecting'
        : 'disconnected',
    username: activeUsername
  });

  socket.on('lf:connect-tiktok', async (input, ack = () => {}) => {
    const username = String(input || '')
      .trim()
      .replace(/^@/, '')
      .replace(/[^\w.]/g, '');

    if (!username) {
      return ack({
        ok: false,
        message: 'اكتب اسم حساب TikTok بدون @ أو معها'
      });
    }

    if (connecting) {
      return ack({
        ok: false,
        message: 'الاتصال جارٍ بالفعل، انتظر قليلاً'
      });
    }

    connecting = true;
    activeUsername = username;

    status('connecting', {
      message: 'جارٍ الاتصال بالبث الحقيقي…'
    });

    try {
      if (liveConnection) {
        const oldConnection = liveConnection;
        liveConnection = null;

        try {
          await oldConnection.disconnect();
        } catch (error) {
          console.warn('Previous connection cleanup:', error?.message || error);
        }
      }

      const conn = new TikTokLiveConnection(username, {
        processInitialData: false
      });

      // Register event handlers before connecting.
      attachEvents(conn);

      const result = await conn.connect();

      liveConnection = conn;
      connecting = false;

      const roomId = result?.roomId || conn.roomId || null;

      status('connected', {
        roomId,
        message: 'تم الاتصال بالبث الحقيقي'
      });

      ack({
        ok: true,
        username,
        roomId
      });
    } catch (error) {
      connecting = false;
      liveConnection = null;

      console.error('TikTok connection failed:', error?.stack || error);

      const message = String(
        error?.message ||
        'تعذّر الاتصال. تأكد أن الحساب يبث حالياً وأن الاتصال بالإنترنت يعمل.'
      );

      status('error', { message });
      ack({ ok: false, message });
    }
  });

  socket.on('lf:disconnect-tiktok', async () => {
    const conn = liveConnection;
    liveConnection = null;
    connecting = false;

    if (conn) {
      try {
        await conn.disconnect();
      } catch (error) {
        console.warn('Disconnect warning:', error?.message || error);
      }
    }

    status('disconnected', { message: 'تم فصل الاتصال' });
  });
});

function localIPs() {
  const out = [];

  for (const list of Object.values(os.networkInterfaces())) {
    for (const info of list || []) {
      if (info.family === 'IPv4' && !info.internal) {
        out.push(info.address);
      }
    }
  }

  return [...new Set(out)];
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\nLiveForge running on port ${PORT}`);

  for (const ip of localIPs()) {
    console.log(`Phone/tablet on same Wi-Fi: http://${ip}:${PORT}`);
  }

  console.log('Press Ctrl+C to stop.');
});
