// index.js

const express = require('express');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const amqp = require('amqplib/callback_api');
const { exec } = require('child_process');
const progress = require('progress-stream');
require('dotenv').config();

// Importação e configuração do Redis
const { createClient } = require('redis');

// Configura o cliente Redis
const redisClient = createClient({
    url: process.env.REDIS_URL,
});

redisClient.on('error', (err) => {
    console.error('Erro ao conectar ao Redis:', err);
});

redisClient.on('connect', () => {
    console.log('Conectado ao Redis');
});

// Inicializa a conexão com o Redis
(async () => {
    try {
        await redisClient.connect();
    } catch (err) {
        console.error('Erro ao conectar ao Redis:', err);
    }
})();

const app = express();
const port = process.env.PORT || 3000;

const API_KEY = process.env.GOOGLE_DRIVE_API_KEY;
const BACKOFF_RETRIES = parseInt(process.env.BACKOFF_RETRIES, 10) || 7;
const videoStoragePath = './public/';
if (!fs.existsSync(videoStoragePath)) {
    fs.mkdirSync(videoStoragePath, { recursive: true });
}

app.use(express.static('public'));
app.use(express.json());

// Configurações do RabbitMQ
const RABBITMQ_HOST = process.env.RABBITMQ_HOST;
const RABBITMQ_PORT = process.env.RABBITMQ_PORT;
const RABBITMQ_USER = process.env.RABBITMQ_USER;
const RABBITMQ_PASS = process.env.RABBITMQ_PASS;
const RABBITMQ_VHOST = process.env.RABBITMQ_VHOST;
const QUEUE_TYPE = process.env.RABBITMQ_QUEUE_TYPE;  // Tipo da fila (lazy ou quorum)
let QUEUE_NAME = process.env.QUEUE_NAME;  // Nome base da fila
const PREFETCH_COUNT = parseInt(process.env.PREFETCH_COUNT, 10); // Prefetch Count configurável
const PROXY_TOKEN = process.env.PROXY_TOKEN;

let channel;
let rabbitmqConnection;

const maxConnectAttempts = 10; // Número máximo de tentativas de reconexão
let connectAttempts = 0;

function startRabbitMQConnection(callback) {
    amqp.connect({
        protocol: 'amqp',
        hostname: RABBITMQ_HOST,
        port: RABBITMQ_PORT,
        username: RABBITMQ_USER,
        password: RABBITMQ_PASS,
        vhost: RABBITMQ_VHOST,
    }, function (err, connection) {
        if (err) {
            console.error('[AMQP] Erro ao conectar:', err.message);
            connectAttempts++;
            if (connectAttempts < maxConnectAttempts) {
                return setTimeout(() => {
                    console.log(`Tentando reconectar... Tentativa ${connectAttempts}/${maxConnectAttempts}`);
                    startRabbitMQConnection(callback);
                }, 1000); // Tenta reconectar após 1 segundo
            } else {
                console.error('Número máximo de tentativas de reconexão atingido.');
                process.exit(1);
            }
        }
        connection.on('error', function (err) {
            if (err.message !== 'Connection closing') {
                console.error('[AMQP] Erro na conexão:', err.message);
            }
        });

        connection.on('close', function () {
            console.error('[AMQP] Conexão fechada.');
            connectAttempts++;
            if (connectAttempts < maxConnectAttempts) {
                return setTimeout(() => {
                    console.log(`Tentando reconectar após fechamento... Tentativa ${connectAttempts}/${maxConnectAttempts}`);
                    startRabbitMQConnection(callback);
                }, 1000);
            } else {
                console.error('Número máximo de tentativas de reconexão após fechamento atingido.');
                process.exit(1);
            }
        });

        console.log('[AMQP] Conectado com sucesso.');
        connectAttempts = 0;
        rabbitmqConnection = connection;
        createChannel(callback);
    });
}

function createChannel(callback) {
    rabbitmqConnection.createChannel(function (err, ch) {
        if (err) {
            console.error('[AMQP] Erro ao criar o canal:', err.message);
            return setTimeout(() => {
                createChannel(callback);
            }, 1000);
        }
        channel = ch;

        channel.on('error', function (err) {
            console.error('[AMQP] Erro no canal:', err.message);
        });

        channel.on('close', function () {
            console.error('[AMQP] Canal fechado.');
            // Tenta recriar o canal
            setTimeout(() => {
                createChannel(callback);
            }, 1000);
        });

        console.log('[AMQP] Canal criado com sucesso.');
        callback();
    });
}

async function fetchWithExponentialBackoff(url, options, retries = BACKOFF_RETRIES) {
    const fetch = (await import('node-fetch')).default;
    let retryCount = 0;
    const maxBackoff = 32000;

    while (retryCount < retries) {
        try {
            const res = await fetch(url, options);
            if (!res.ok) {
                throw new Error(`HTTP error! status: ${res.status}`);
            }
            return res;
        } catch (error) {
            const waitTime = Math.min(
                Math.pow(2, retryCount) * 1000 + Math.floor(Math.random() * 1000),
                maxBackoff
            );
            console.log(`Tentando novamente em ${waitTime} ms...`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            retryCount++;
        }
    }
    throw new Error(`Falha ao buscar ${url} após ${retries} tentativas`);
}

async function downloadVideo(fileUrl, filePath, index, total) {
    const fetch = (await import('node-fetch')).default;
    const res = await fetchWithExponentialBackoff(fileUrl, {}, BACKOFF_RETRIES);

    const totalSize = res.headers.get('content-length');
    const str = progress({
        length: totalSize,
        time: 100 /* ms */
    });

    str.on('progress', function (progress) {
        console.log(`Baixando vídeo ${index + 1}/${total}: ${Math.round(progress.percentage)}%`);
    });

    const dest = fs.createWriteStream(filePath);
    res.body.pipe(str).pipe(dest);

    await new Promise((resolve, reject) => {
        dest.on('finish', resolve);
        dest.on('error', reject);
    });

    const stats = fs.statSync(filePath);
    console.log(`Vídeo ${index + 1}/${total} tamanho: ${stats.size} bytes`);
    if (stats.size > 16 * 1024 * 1024) { // 16 MB
        const timestamp = new Date().toISOString().replace(/[-:.]/g, "");
        const newPath = path.join(
            videoStoragePath,
            `${path.basename(filePath, path.extname(filePath))}_${timestamp}_converted.mp4`
        );
        const convertedPath = await convertVideo(filePath, newPath, index, total, [480, 320, 144]);
        fs.unlinkSync(filePath);
        if (convertedPath) {
            return convertedPath;
        } else {
            console.log(`Vídeo ${index + 1}/${total} ignorado por ser maior que 16 MB após todas as conversões.`);
            return null;
        }
    }

    const finalPath = path.join(videoStoragePath, path.basename(filePath));
    fs.renameSync(filePath, finalPath);
    console.log(`Vídeo ${index + 1}/${total} baixado (${Math.round(((index + 1) / total) * 100)}%)`);
    return finalPath;
}

async function convertVideo(inputPath, outputPath, index, total, widths) {
    for (const width of widths) {
        await new Promise((resolve, reject) => {
            const command = `ffmpeg -i "${inputPath}" -vf "scale=${width}:-2" -b:v 1M "${outputPath}"`;
            console.log(`Executando comando: ${command}`);
            const ffmpegProcess = exec(command, (error, stdout, stderr) => {
                if (error) {
                    console.error(`Erro ao converter o vídeo para ${width}px: ${stderr}`);
                    reject(error);
                } else {
                    resolve();
                }
            });

            ffmpegProcess.stderr.on('data', (data) => {
                const progress = parseFfmpegProgress(data.toString());
                if (progress) {
                    console.log(`Convertendo vídeo ${index + 1}/${total} para ${width}px: ${progress}%`);
                }
            });
        });

        const stats = fs.statSync(outputPath);
        console.log(`Tamanho do vídeo convertido para ${width}px: ${stats.size} bytes`);
        if (stats.size <= 16 * 1024 * 1024) { // 16 MB
            console.log(`Vídeo ${index + 1}/${total} convertido para ${width}px (${Math.round(((index + 1) / total) * 100)}%)`);
            return outputPath;
        } else {
            console.log(`Tamanho do vídeo convertido para ${width}px ainda é muito grande: ${stats.size} bytes`);
        }
    }
    fs.unlinkSync(outputPath);
    return null;
}

function parseFfmpegProgress(data) {
    const match = data.match(/frame=.*time=(\d+:\d+:\d+)/);
    if (match) {
        const timeParts = match[1].split(':');
        const totalSeconds = (+timeParts[0]) * 3600 + (+timeParts[1]) * 60 + (+timeParts[2]);
        const duration = 3600; // Assumindo 1 hora de vídeo para simplificar a porcentagem
        return Math.round((totalSeconds / duration) * 100);
    }
    return null;
}

async function getVideoUrlsFromFolder(folderId) {
    const fetch = (await import('node-fetch')).default;
    const url = `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents+and+(mimeType='video/mp4'+or+mimeType='video/avi'+or+mimeType='video/mkv')&key=${API_KEY}&fields=files(id,name,mimeType)`;
    const res = await fetchWithExponentialBackoff(url, {}, BACKOFF_RETRIES);
    const data = await res.json();
    if (!data.files || data.files.length === 0) {
        throw new Error('Nenhum vídeo encontrado na pasta especificada.');
    }
    return data.files.map(file => ({
        url: `https://drive.google.com/uc?id=${file.id}`,
        name: file.name
    }));
}

async function processVideoCreation(msg, attempt = 0, currentIndex = 0, log = '') {
    const { link, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf } = JSON.parse(msg.content.toString());

    try {
        const isFolderLink = link.includes('/folders/');
        const folderIdOrFileId = extractIdFromLink(link);
        let videos = [];

        if (isFolderLink) {
            videos = await getVideoUrlsFromFolder(folderIdOrFileId);
        } else {
            const videoUrl = `https://drive.google.com/uc?id=${folderIdOrFileId}`;
            videos = [{ url: videoUrl, name: 'downloaded.mp4' }];
        }

        if (videos.length === 0) {
            throw new Error('Nenhum vídeo encontrado na pasta ou arquivo especificado.');
        }

        const videoPaths = [];
        for (let i = currentIndex; i < videos.length; i++) {
            const videoPath = path.join(__dirname, `${videos[i].name}`);
            const finalPath = await downloadVideo(videos[i].url, videoPath, i, videos.length);
            if (finalPath) {
                videoPaths.push(finalPath);
            } else {
                throw new Error(`Erro ao processar o vídeo ${i + 1}/${videos.length}`);
            }
        }

        const videoNames = videoPaths.map(v => path.basename(v));
        console.log(`Vídeos baixados e processados: ${videoNames.join(', ')}`);

        // Agendar para apagar os vídeos após 10 minutos
        setTimeout(() => {
            videoPaths.forEach(videoPath => {
                fs.unlink(videoPath, (err) => {
                    if (err) {
                        console.error(`Erro ao apagar o vídeo (${videoPath}):`, err);
                    } else {
                        console.log(`Vídeo (${videoPath}) apagado com sucesso.`);
                    }
                });
            });
        }, 600000); // 600000 milissegundos = 10 minutos

        await axios.post('https://ultra-n8n.neuralbase.com.br/webhook/videos', {
            videoNames,
            Id,
            context,
            UserMsg,
            MsgIdPhoto, 
            MsgIdVideo, 
            MsgIdPdf,
            link,
            result: true
        }).then(() => {
            console.log(`Webhook enviado sem erros`);
        }).catch(error => {
            console.error(`Erro ao enviar webhook: ${error}`);
        });

        channel.ack(msg);
    } catch (error) {
        console.error('Erro ao processar o vídeo:', error);
        log += `Erro ao processar o vídeo: ${error.message}\n    at ${error.stack}\n`;

        if (attempt < BACKOFF_RETRIES) {
            const waitTime = Math.min(Math.pow(2, attempt) * 1000 + Math.floor(Math.random() * 1000), 32000);
            console.log(`Tentando novamente processVideoCreation em ${waitTime} ms... (tentativa ${attempt + 1}/${BACKOFF_RETRIES})`);
            setTimeout(() => processVideoCreation(msg, attempt + 1, currentIndex, log), waitTime);
        } else {
            await axios.post('https://ultra-n8n.neuralbase.com.br/webhook/videos', {
                videoNames: null,
                Id,
                context,
                UserMsg,
                MsgIdPhoto, 
                MsgIdVideo, 
                MsgIdPdf,
                link,
                result: false,
                reason: log
            }).then(() => {
                console.log(`Webhook enviado com erros`);
            }).catch(error => {
                console.error(`Erro ao enviar webhook: ${error}`);
            });

            channel.nack(msg, false, false); // Rejeita a mensagem sem reencaminhar
        }
    }
}

function extractIdFromLink(link) {
    const fileIdMatch = link.match(/\/d\/([a-zA-Z0-9-_]+)/);
    const folderIdMatch = link.match(/\/folders\/([a-zA-Z0-9-_]+)/);

    if (fileIdMatch) {
        return fileIdMatch[1];
    } else if (folderIdMatch) {
        return folderIdMatch[1];
    } else {
        return null;
    }
}

function setupConsumer() {
    channel.consume(QUEUE_NAME, async (msg) => {
        try {
            await processVideoCreation(msg);
        } catch (error) {
            console.error('Erro ao processar a mensagem:', error);
            channel.nack(msg, false, false); // Rejeita a mensagem sem reencaminhar
        }
    }, { noAck: false });
}

// Inicia a conexão e, após estabelecida, configura o consumidor
startRabbitMQConnection(() => {
    console.log('Iniciando o consumidor...');
    setupConsumer();
});

app.post('/create-video', async (req, res) => {
    const { link, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf } = req.body;

    if (!link || !Id) {
        return res.status(400).send('Parâmetros ausentes: link e Id são necessários.');
    }

    // Implementação do controle de duplicação com Redis
    const duplicateKey = `video_request:${Id}:${link}`;
    try {
        // Verifica se já existe um registro com o mesmo Id e link
        const exists = await redisClient.get(duplicateKey);

        if (exists) {
            console.log(`Solicitação ignorada para Id: ${Id}, Link: ${link} (dentro da janela de 3 minutos)`);
            return res.send({ message: 'Solicitação ignorada (já processada nos últimos 3 minutos).' });
        } else {
            // Armazena no Redis com TTL de 3 minutos (180 segundos)
            await redisClient.setEx(duplicateKey, 180, 'processed');

            const msg = { link, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf };
            channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(msg)), {
                persistent: true
            });

            console.log('Mensagem enviada para a fila');
            res.send({ message: 'Iniciando criação dos vídeos.' });
        }
    } catch (error) {
        console.error('Erro ao acessar o Redis:', error);
        res.status(500).send('Erro interno do servidor.');
    }
});

app.get('/download', (req, res) => {
    const { videoName } = req.query;

    if (!videoName) {
        return res.status(400).send('Nome do vídeo não especificado.');
    }

    const filePath = path.join(videoStoragePath, videoName);

    if (!fs.existsSync(filePath)) {
        return res.status(404).send('Vídeo não encontrado.');
    }

    res.download(filePath, videoName, (err) => {
        if (err) {
            console.error(`Erro ao baixar o vídeo (${videoName}):`, err);
        }
    });
});

app.listen(port, () => {
    console.log(`Servidor rodando em http://localhost:${port}`);
});

// Força Garbage Collection se disponível
if (global.gc) {
    setInterval(() => {
        console.log('Forçando Garbage Collection...');
        global.gc();
    }, 60000); // Executa a cada 60 segundos
} else {
    console.warn('Garbage collector não disponível. Execute com --expose-gc');
}
