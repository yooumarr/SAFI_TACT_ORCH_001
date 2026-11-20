FROM node:24-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src
USER root
RUN mkdir -p /data && chown -R node:node /data
USER node
EXPOSE 8787
CMD ["node", "src/server.js"]
