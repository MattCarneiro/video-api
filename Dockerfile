# Use uma imagem base oficial do Node.js
FROM node:18

# Instalar dependências do sistema e Chromium
RUN apt-get update && apt-get upgrade -y && apt install -y nano && apt-get install -y \
    ffmpeg \
    --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Definir variáveis de ambiente necessárias para o Puppeteer
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Criar diretório de trabalho
WORKDIR /app

# Clonar o repositório
RUN apt-get update && apt-get install -y git \
    && git clone https://github.com/easypanel-io/express-js-sample.git . \
    && apt-get remove -y git \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copiar package.json e package-lock.json
COPY package*.json ./

# Instalar dependências da aplicação
RUN npm install express pdf-lib fs node-fetch sharp path axios amqp amqplib dotenv child_process progress-stream

# Copiar o código adicional da aplicação
COPY . .

# Expor a porta em que a aplicação vai rodar
EXPOSE 3000

# Comando para iniciar a aplicação
CMD ["node", "index.js"]
