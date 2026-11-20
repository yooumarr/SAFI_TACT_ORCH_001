import { connect, RetentionPolicy, StorageType } from 'nats';

const STREAM = 'ORCH_CONTROL';
const SUBJECT = 'orch.control.events';
const codec = new TextEncoder();
const decoder = new TextDecoder();

export class JetStreamEventJournal {
  constructor({ servers, name = STREAM }) {
    this.servers = servers;
    this.name = name;
    this.connection = null;
    this.js = null;
    this.jsManager = null;
  }

  async connect() {
    this.connection = await connect({ servers: this.servers, name: 'local-orchestration-gate' });
    this.js = this.connection.jetstream();
    this.jsManager = await this.connection.jetstreamManager();
    try {
      await this.jsManager.streams.info(this.name);
    } catch (error) {
      if (error.code !== '404' && error.code !== 'STREAM_NOT_FOUND') throw error;
      await this.jsManager.streams.add({
        name: this.name,
        subjects: [SUBJECT],
        storage: StorageType.File,
        retention: RetentionPolicy.Limits,
        max_age: 30 * 24 * 60 * 60 * 1_000_000_000,
        duplicate_window: 2 * 60 * 1_000_000_000,
        max_msgs: 100_000,
      });
    }
  }

  async append(event) {
    if (!this.js) throw new Error('JetStream event journal is not connected');
    await this.js.publish(SUBJECT, codec.encode(JSON.stringify(event)), { msgID: event.eventId });
  }

  async readConversation(conversationId) {
    if (!this.jsManager) throw new Error('JetStream event journal is not connected');
    const info = await this.jsManager.streams.info(this.name);
    const lastSequence = info.state.last_seq;
    const events = [];
    const pageSize = 256;
    let startSequence = 1;
    while (startSequence <= lastSequence && events.length < pageSize) {
      const page = await this.jsManager.streams.getMessage(this.name, { seq: startSequence }).catch((error) => {
        if (error.code === '404' || error.code === 'MESSAGE_NOT_FOUND') return null;
        throw error;
      });
      const storedMessage = page?.smr?.message ?? page?.message;
      if (storedMessage) {
        const event = JSON.parse(decoder.decode(typeof storedMessage.data === 'string' ? Buffer.from(storedMessage.data, 'base64') : storedMessage.data));
        if (event.conversationId === conversationId) events.push(event);
        startSequence = storedMessage.seq + 1;
      } else {
        startSequence += 1;
      }
    }
    return events;
  }

  async close() {
    await this.connection?.drain();
  }
}
