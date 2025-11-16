const net = require('net');
const { spawn } = require('child_process');

class KVDBClient {
  constructor(host = 'localhost', port = 6969) {
    this.host = host;
    this.port = port;
    this.socket = null;
    this.connected = false;
  }

  async connect() {
    if (this.connected) return;

    return new Promise((resolve, reject) => {
      this.socket = new net.Socket();
      this.socket.setKeepAlive(true);
      this.socket.setNoDelay(true);

      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, 5000);

      this.socket.connect(this.port, this.host, () => {
        clearTimeout(timeout);
        this.connected = true;
        resolve();
      });

      this.socket.once('error', (err) => {
        clearTimeout(timeout);
        this.connected = false;
        reject(err);
      });
    });
  }

  disconnect() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
      this.connected = false;
    }
  }

  sendCommand(command) {
    return new Promise(async (resolve, reject) => {
      if (!this.socket) {
        try {
          await this.connect();
        } catch (err) {
          return reject(err);
        }
      }

      let response = '';

      const dataHandler = (data) => {
        response += data.toString();

        if (response.includes('\r')) {
          this.socket.removeListener('data', dataHandler);
          clearTimeout(timeout);
          const result = response.replace(/\r/g, '').trim();
          resolve(result);
        }
      };

      this.socket.on('data', dataHandler);

      const timeout = setTimeout(() => {
        this.socket.removeListener('data', dataHandler);
        reject(new Error('Timeout após 5s'));
      }, 5000);

      this.socket.write(command, (err) => {
        if (err) {
          this.socket.removeListener('data', dataHandler);
          clearTimeout(timeout);
          reject(err);
        }
      });
    });
  }

  async write(key, value) {
    return this.sendCommand(`write ${key}|${value}\r`);
  }

  async read(key) {
    return this.sendCommand(`read ${key}\r`);
  }

  async delete(key) {
    return this.sendCommand(`delete ${key}\r`);
  }
}

async function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function testPersistence(port, binaryPath, waitBeforeKill, waitBeforeLaunch, numRecords = 100) {
  console.log('\n╔════════════════════════════════════════════════════╗');
  console.log('║           PIZZAKV - TESTE DE PERSISTÊNCIA         ║');
  console.log('╚════════════════════════════════════════════════════╝\n');

  console.log('Configuração:');
  console.log(`  Porta: ${port}`);
  console.log(`  Binário: ${binaryPath}`);
  console.log(`  Registros: ${numRecords}`);
  console.log(`  Tempo antes de matar: ${waitBeforeKill}ms`);
  console.log(`  Tempo antes de relançar: ${waitBeforeLaunch}ms\n`);

  const testData = new Map();
  for (let i = 0; i < numRecords; i++) {
    testData.set(`persist:${i}`, `value_${i}_${Date.now()}`);
  }

  // Select 20% of records to delete
  const keysToDelete = new Set();
  const deleteCount = Math.floor(numRecords * 0.2);
  while (keysToDelete.size < deleteCount) {
    const randomIndex = Math.floor(Math.random() * numRecords);
    keysToDelete.add(`persist:${randomIndex}`);
  }

  console.log('─'.repeat(60));
  console.log('FASE 1: Iniciando servidor e escrevendo dados\n');

  const fs = require('fs');
  try {
    fs.unlinkSync('.db');
    console.log('✓ Arquivo .db removido\n');
  } catch (err) {
    if (err.code !== 'ENOENT') {
      console.log(`⚠ Erro ao remover .db: ${err.message}\n`);
    }
  }

  const serverProcess = spawn(binaryPath, [port.toString()], {
    stdio: ['ignore', 'pipe', 'pipe']
  });

  let serverOutput = '';
  serverProcess.stdout.on('data', (data) => {
    serverOutput += data.toString();
  });

  serverProcess.stderr.on('data', (data) => {
    console.log(`[Server Error]: ${data.toString().trim()}`);
  });

  console.log(`✓ Servidor iniciado (PID: ${serverProcess.pid})`);
  console.log('  Aguardando servidor ficar pronto...');

  await wait(1000);

  const client = new KVDBClient('localhost', port);

  try {
    await client.connect();
    console.log('✓ Conexão estabelecida\n');

    console.log(`Escrevendo ${numRecords} registros...`);
    let writeSuccess = 0;
    let writeErrors = 0;

    for (const [key, value] of testData) {
      try {
        await client.write(key, value);
        writeSuccess++;

        if (writeSuccess % 10 === 0) {
          process.stdout.write(`\r  Progresso: ${writeSuccess}/${numRecords} (${((writeSuccess/numRecords)*100).toFixed(0)}%)`);
        }
      } catch (err) {
        writeErrors++;
        console.error(`\n  Erro ao escrever ${key}: ${err.message}`);
      }
    }

    console.log(`\n✓ Escrita concluída: ${writeSuccess} sucessos, ${writeErrors} erros\n`);

    console.log(`Deletando ${keysToDelete.size} registros aleatórios...`);
    let deleteSuccess = 0;
    let deleteErrors = 0;

    for (const key of keysToDelete) {
      try {
        await client.delete(key);
        deleteSuccess++;
        testData.delete(key); // Remove from expected data

        if (deleteSuccess % 5 === 0) {
          process.stdout.write(`\r  Progresso: ${deleteSuccess}/${keysToDelete.size} (${((deleteSuccess/keysToDelete.size)*100).toFixed(0)}%)`);
        }
      } catch (err) {
        deleteErrors++;
        console.error(`\n  Erro ao deletar ${key}: ${err.message}`);
      }
    }

    console.log(`\n✓ Deleção concluída: ${deleteSuccess} sucessos, ${deleteErrors} erros`);
    console.log(`  Registros restantes esperados: ${testData.size}\n`);

    client.disconnect();

    console.log(`Aguardando ${waitBeforeKill}ms antes de matar o servidor...`);
    await wait(waitBeforeKill);

    console.log('─'.repeat(60));
    console.log('FASE 2: Matando servidor\n');

    serverProcess.kill('SIGTERM');

    await new Promise((resolve) => {
      serverProcess.on('exit', (code, signal) => {
        console.log(`✓ Servidor finalizado (código: ${code}, sinal: ${signal})\n`);
        resolve();
      });

      setTimeout(() => {
        if (!serverProcess.killed) {
          console.log('⚠ Servidor não respondeu ao SIGTERM, forçando SIGKILL...');
          serverProcess.kill('SIGKILL');
        }
      }, 2000);
    });

    console.log(`Aguardando 1 segundo antes de relançar...`);
    await wait(1000);

    console.log('─'.repeat(60));
    console.log('FASE 3: Relançando servidor e validando dados\n');

    const serverProcess2 = spawn(binaryPath, [port.toString()], {
      stdio: ['ignore', 'pipe', 'pipe']
    });

    serverProcess2.stdout.on('data', (data) => {
      serverOutput += data.toString();
    });

    serverProcess2.stderr.on('data', (data) => {
      console.log(`[Server Error]: ${data.toString().trim()}`);
    });

    console.log(`✓ Servidor relançado (PID: ${serverProcess2.pid})`);
    console.log(`  Aguardando ${waitBeforeLaunch}ms antes de enviar comandos...`);

    await wait(waitBeforeLaunch);

    const client2 = new KVDBClient('localhost', port);
    await client2.connect();
    console.log('✓ Nova conexão estabelecida\n');

    console.log(`Lendo e validando ${numRecords} registros...`);
    let readSuccess = 0;
    let readErrors = 0;
    let validationErrors = 0;

    for (const [key, expectedValue] of testData) {
      try {
        const actualValue = await client2.read(key);

        if (actualValue === 'error') {
          validationErrors++;
          if (validationErrors <= 5) {
            console.error(`\n  ✗ Chave não encontrada: ${key}`);
          }
        } else if (actualValue !== expectedValue) {
          validationErrors++;
          if (validationErrors <= 5) {
            console.error(`\n  ✗ Valor incorreto para ${key}:`);
            console.error(`    Esperado: "${expectedValue}"`);
            console.error(`    Recebido: "${actualValue}"`);
          }
        } else {
          readSuccess++;
        }

        if ((readSuccess + validationErrors) % 10 === 0) {
          const total = readSuccess + validationErrors;
          process.stdout.write(`\r  Progresso: ${total}/${numRecords} (${((total/numRecords)*100).toFixed(0)}%) | OK: ${readSuccess} | Erros: ${validationErrors}`);
        }
      } catch (err) {
        readErrors++;
        console.error(`\n  Erro ao ler ${key}: ${err.message}`);
      }
    }

    console.log('\n');
    client2.disconnect();
    serverProcess2.kill('SIGTERM');

    await new Promise((resolve) => {
      serverProcess2.on('exit', () => resolve());
      setTimeout(() => {
        if (!serverProcess2.killed) {
          serverProcess2.kill('SIGKILL');
        }
      }, 2000);
    });

    console.log('─'.repeat(60));
    console.log('RESULTADOS:\n');
    console.log(`  Total de registros escritos: ${numRecords}`);
    console.log(`  Registros deletados: ${keysToDelete.size}`);
    console.log(`  Registros esperados: ${testData.size}`);
    console.log(`  Escritas bem-sucedidas: ${writeSuccess}`);
    console.log(`  Leituras bem-sucedidas: ${readSuccess}`);
    console.log(`  Erros de validação: ${validationErrors}`);
    console.log(`  Erros de leitura: ${readErrors}`);

    const persistenceRate = ((readSuccess / testData.size) * 100).toFixed(2);
    console.log(`\n  Taxa de persistência: ${persistenceRate}%`);

    console.log('─'.repeat(60));

    if (validationErrors === 0 && readErrors === 0) {
      console.log('\n✓✓✓ TESTE DE PERSISTÊNCIA PASSOU ✓✓✓\n');
      return true;
    } else {
      console.log('\n✗✗✗ TESTE DE PERSISTÊNCIA FALHOU ✗✗✗\n');
      return false;
    }

  } catch (err) {
    console.error(`\n✗ ERRO FATAL: ${err.message}\n`);

    try { client.disconnect(); } catch {}
    try { serverProcess.kill('SIGKILL'); } catch {}

    return false;
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
const port = parseInt(args[0]) || 6969;
const binaryPath = args[1] || './pizzakv';
const waitBeforeKill = parseInt(args[2]) || 1000;
const waitBeforeLaunch = parseInt(args[3]) || 1000;
const numRecords = parseInt(args[4]) || 100;

if (args.includes('--help') || args.includes('-h')) {
  console.log('\nUsage: node test_persistence.js [port] [binary] [waitBeforeKill] [waitBeforeLaunch] [numRecords]\n');
  console.log('Arguments:');
  console.log('  port              - Port to run the server on (default: 6969)');
  console.log('  binary            - Path to the pizzakv binary (default: ./pizzakv)');
  console.log('  waitBeforeKill    - Milliseconds to wait before killing server (default: 1000)');
  console.log('  waitBeforeLaunch  - Milliseconds to wait before relaunching (default: 1000)');
  console.log('  numRecords        - Number of records to test (default: 100)\n');
  console.log('Example:');
  console.log('  node test_persistence.js 6969 ./pizzakv 2000 1500 200\n');
  process.exit(0);
}

testPersistence(port, binaryPath, waitBeforeKill, waitBeforeLaunch, numRecords)
  .then(success => process.exit(success ? 0 : 1))
  .catch(err => {
    console.error(err);
    process.exit(1);
  });