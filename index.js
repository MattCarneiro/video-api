// index.js
require('dotenv').config();
const express = require('express');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const amqp = require('amqplib/callback_api');
const PDFDocument = require('pdfkit');
const FormData = require('form-data');
// Ajuste no import do image-size
let sizeOf = require('image-size');
if (sizeOf.default) sizeOf = sizeOf.default;

// Dependência para conversão de imagens
const sharp = require('sharp');

const app = express();
const port = process.env.PORT || 3000;

// RabbitMQ config
const RABBITMQ_HOST    = process.env.RABBITMQ_HOST;
const RABBITMQ_PORT    = process.env.RABBITMQ_PORT;
const RABBITMQ_USER    = process.env.RABBITMQ_USER;
const RABBITMQ_PASS    = process.env.RABBITMQ_PASS;
const RABBITMQ_VHOST   = process.env.RABBITMQ_VHOST;
const QUEUE_NAME       = process.env.QUEUE_NAME;
const PREFETCH_COUNT   = parseInt(process.env.PREFETCH_COUNT, 10) || 1;

// General retries / backoff
const BACKOFF_RETRIES  = parseInt(process.env.BACKOFF_RETRIES, 10) || 7;
const MAX_BACKOFF_MS   = 32000;

// Auth for image download and CRM patch
const API_USER         = process.env.BASIC_AUTH_USER || 'corporativo@neuralbase.com.br';
const API_PASS         = process.env.BASIC_AUTH_PASS || 'Hunted123@@321';

// Upload PDF
const MEDIA_UPLOAD_URL = process.env.MEDIA_UPLOAD_URL   || 'https://media.neuralbroker.com.br/api/upload';
const MEDIA_AUTH_TOKEN = process.env.MEDIA_AUTH_TOKEN   || 'g3cetNqmuXSgwHGpXU95LyGw.MTcxNzQ2NzExNjAzMw';

// Temp folder
const tmpDir = path.resolve(__dirname, 'tmp');
if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });

app.use(express.json());

let channel;
let connection;           // <<< Adicionado >>>
let reconnectAttempts = 1;

/** Sleep helper */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Exponential backoff fetch */
async function fetchWithBackoff(url, options = {}) {
  for (let i = 0; i < BACKOFF_RETRIES; i++) {
    try {
      const { default: fetch } = await import('node-fetch');
      const res = await fetch(url, options);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res;
    } catch (err) {
      const wait = Math.min(2 ** i * 1000 + Math.random() * 1000, MAX_BACKOFF_MS);
      console.log(`fetch retry #${i + 1} in ${Math.round(wait)}ms`);
      await sleep(wait);
    }
  }
  throw new Error(`Failed to fetch ${url} after ${BACKOFF_RETRIES} attempts`);
}

/** RabbitMQ connect with backoff */
function startRabbitMQConnection() {
  amqp.connect({
    protocol: 'amqp',
    hostname: RABBITMQ_HOST,
    port:     RABBITMQ_PORT,
    username: RABBITMQ_USER,
    password: RABBITMQ_PASS,
    vhost:    RABBITMQ_VHOST,
  }, (err, conn) => {
    if (err) {
      console.error('[AMQP] connect error:', err.message);
      return scheduleReconnect();
    }
    connection = conn; // <<< Adicionado >>>
    conn.on('error', () => scheduleReconnect());
    conn.on('close', () => scheduleReconnect());
    conn.createChannel((err, ch) => {
      if (err) {
        console.error('[AMQP] channel error:', err.message);
        return scheduleReconnect();
      }
      channel = ch;
      channel.prefetch(PREFETCH_COUNT);
      reconnectAttempts = 1;
      console.log('[AMQP] connected & channel created');
      channel.consume(QUEUE_NAME, msg => processJob(msg), { noAck: false });
    });
  });
}

function scheduleReconnect() {
  const delay = Math.min(2 ** reconnectAttempts++ * 1000, MAX_BACKOFF_MS);
  console.log(`[AMQP] reconnecting in ${delay}ms`);
  setTimeout(startRabbitMQConnection, delay);
}

/** Generate random filename or ID */
function randomString(len) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  return Array.from({length: len}, () => chars.charAt(Math.floor(Math.random() * chars.length))).join('');
}

/** Patch CRM with generic error message */
async function patchCrmError(Id) {
  try {
    await axios.patch(
      `https://crm.neuralbroker.com.br/api/v1/CImovel/${Id}`,
      { linkphoto: 'Erro! Tente Novamente em Instantes.' },
      {
        auth: { username: API_USER, password: API_PASS },
        headers: { 'X-Skip-Duplicate-Check': 'true' }
      }
    );
    console.log('CRM patched with error message');
  } catch (err) {
    console.error('Error patching CRM error message:', err.message);
  }
}

/** Helper para detectar tipo de imagem via import dinâmico */
async function detectFileType(buffer) {
  const { fileTypeFromBuffer } = await import('file-type');
  return fileTypeFromBuffer(buffer);
}

/** Main processor */
async function processJob(msg) {
  const {
    array,
    Id,
    context,
    UserMsg,
    MsgIdPhoto,
    MsgIdVideo,
    MsgIdPdf,
    photoId = ''
  } = JSON.parse(msg.content.toString());

  console.log(`Starting job for Id=${Id}, ${array.length} images, photoId=${photoId}`);

  // 1) download images
  const buffers = [];
  for (let i = 0; i < array.length; i++) {
    const code = array[i];
    try {
      console.log(`Downloading image ${i+1}/${array.length} (id=${code})`);
      const res = await fetchWithBackoff(
        `https://crm.neuralbroker.com.br/?entryPoint=download&id=${code}`,
        {
          headers: {
            'Authorization': 'Basic ' + Buffer.from(`${API_USER}:${API_PASS}`).toString('base64')
          }
        }
      );
      const contentType = res.headers.get('content-type') || '';
      console.log(`Content-Type da imagem ${i+1}:`, contentType);
      const buf = Buffer.from(await res.arrayBuffer());
      buffers.push(buf);
      console.log(`Downloaded ${i+1}/${array.length}`);
    } catch (err) {
      console.warn(`Failed to download image ${i+1}, skipping:`, err.message);
    }
  }

  if (buffers.length === 0) {
    console.error('No images downloaded, aborting PDF');
    await patchCrmError(Id);
    channel.ack(msg);
    return;
  }

  // 2) optional Attachment upload
  let presentationId = photoId;
  if (!presentationId) {
    const pick = buffers.length > 5
      ? buffers[Math.floor(Math.random() * 5)]
      : buffers[Math.floor(Math.random() * buffers.length)];
    const dataUri = `data:image/png;base64,${pick.toString('base64')}`;
    console.log('Uploading one image to Attachment API');
    const attRes = await axios.post(
      'https://crm.neuralbroker.com.br/api/v1/Attachment',
      {
        role: 'Attachment',
        relatedType: 'CImovel',
        field: 'presentation',
        file: dataUri,
        type: 'image/jpeg',
        name: randomString(10)
      },
      { auth: { username: API_USER, password: API_PASS } }
    );
    presentationId = attRes.data.id;
    console.log('Received presentationId:', presentationId);
  }

  // 3) Build PDF — página do tamanho exato da imagem
  const pdfName = `${randomString(15)}.pdf`;
  const filePath = path.join(tmpDir, pdfName);
  console.log(`Building PDF (${buffers.length} pages)`);
  const doc = new PDFDocument({ autoFirstPage: false });
  const ws = fs.createWriteStream(filePath);
  doc.pipe(ws);

  for (let idx = 0; idx < buffers.length; idx++) {
    let imgBuf = buffers[idx];
    // detecta formato real
    const type = await detectFileType(imgBuf);
    if (!type || !['png','jpg','jpeg'].includes(type.ext)) {
      console.log(`Convertendo imagem ${idx+1} (${type?.mime}) para PNG`);
      imgBuf = await sharp(imgBuf).png().toBuffer();
    }
    const { width, height } = sizeOf(imgBuf);
    doc.addPage({ size: [width, height], margin: 0 });
    doc.image(imgBuf, 0, 0, { width, height });
    console.log(`Added image ${idx+1}/${buffers.length} to PDF (${width}×${height})`);
  }

  doc.end();
  await new Promise((res, rej) => ws.on('finish', res).on('error', rej));
  console.log('PDF built at', filePath);

  // 4) Upload PDF
  console.log('Uploading PDF to media API');
  const form = new FormData();
  form.append('file', fs.createReadStream(filePath), { filename: pdfName, contentType: 'application/pdf' });
  let uploadRes;
  try {
    uploadRes = await axios.post(MEDIA_UPLOAD_URL, form, {
      headers: { ...form.getHeaders(), 'X-Zipline-Filename': pdfName, 'Authorization': MEDIA_AUTH_TOKEN }
    });
  } catch (err) {
    console.error('Upload failed:', err.message);
    return retryOrFail(msg, filePath);
  }

  if (uploadRes.status !== 200 || !uploadRes.data.files?.[0]) {
    console.error('Upload response invalid:', uploadRes.status);
    return retryOrFail(msg, filePath);
  }

  const rawLink = uploadRes.data.files[0];
  const linkphoto = rawLink.replace(/\/u\//, '/r/');
  console.log('Uploaded PDF URL:', linkphoto);

  // 5) Patch CRM
  const patchData = { linkphoto, photomatrix: array };
  if (!photoId) patchData.presentationId = presentationId;
  console.log('Patching CRM with', patchData);

  let success = false;
  for (let i = 0; i < BACKOFF_RETRIES; i++) {
    try {
      await axios.patch(
        `https://crm.neuralbroker.com.br/api/v1/CImovel/${Id}`,
        patchData,
        {
          auth: { username: API_USER, password: API_PASS },
          headers: { 'X-Skip-Duplicate-Check': 'true' }
        }
      );
      success = true;
      console.log('CRM patched successfully');
      break;
    } catch (err) {
      console.warn(`CRM patch attempt ${i+1} failed:`, err.message);
      if (i < BACKOFF_RETRIES - 1) {
        await sleep(Math.min(2 ** i * 1000 + Math.random() * 1000, MAX_BACKOFF_MS));
      }
    }
  }
  if (!success) console.error('CRM patch failed after retries');

  channel.ack(msg);
  fs.unlink(filePath, () => {});
}

/** Retry entire job or final fail */
function retryOrFail(msg, filePath) {
  return (function attempt(attempt = 1) {
    if (attempt < BACKOFF_RETRIES) {
      const wait = Math.min(2 ** attempt * 1000 + Math.random() * 1000, MAX_BACKOFF_MS);
      console.log(`Retrying job in ${wait}ms (attempt ${attempt+1}/${BACKOFF_RETRIES})`);
      return setTimeout(() => processJob(msg), wait);
    } else {
      console.error('Job failed after retries, sending CRM error');
      patchCrmError(JSON.parse(msg.content.toString()).Id)
        .finally(() => {
          channel.nack(msg, false, false);
          if (filePath) fs.unlink(filePath, () => {});
        });
    }
  })(1);
}

// Start RabbitMQ
startRabbitMQConnection();

// enqueue endpoint
app.post('/download-pdfs', (req, res) => {
  if (!channel) {
    return res.status(500).send('RabbitMQ channel not ready');
  }
  const { array, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf, photoId = '' } = req.body;
  if (!array || !Id) {
    return res.status(400).send('array and Id required');
  }
  channel.sendToQueue(
    QUEUE_NAME,
    Buffer.from(JSON.stringify({ array, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf, photoId })),
    { persistent: true }
  );
  res.send({ message: 'Job queued' });
});

// <<< Adicionado: healthz endpoint >>>
app.get('/healthz', (req, res) =>
  res.status(200).json({ status: 'ok' })
);

// <<< Adicionado: readiness endpoint >>>
app.get('/readyz', (req, res) =>
  res.status(200).json({ status: 'ready' })
);

// <<< Adicionado: graceful shutdown >>>
const server = app.listen(port, () => console.log(`Server running on port ${port}`));
function shutdown() {
  console.log('Received shutdown signal, shutting down gracefully');
  channel && channel.close();
  connection && connection.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10000);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Force GC every minute if available
if (global.gc) {
  setInterval(() => { console.log('Forçando GC...'); global.gc(); }, 60000);
} else {
  console.warn('GC not available; run with --expose-gc');
}
