import * as os from 'node:os';
import * as fsp from 'node:fs/promises';

console.time();

const fname = '/tmp/measurements.txt';

const fsize = Bun.file(fname).size;

const threads = os.cpus().length;
// console.log(fsize, threads );

// chunks.length = threads + 1
let chunks = await process_chunk(0 , fsize, threads);
// console.log( chunks );

// Map<string, {min: number, max: number, sum: number, count: number}>
const map = new Map();

let Workers = 1;
for (let i = 1; i < chunks.length; i++) {
    const worker = new Worker('./yi_worker.js');

    worker.postMessage({ start : chunks[i - 1], end : chunks[i], threads : threads });
    worker.onmessage = (event) => {  
        let chunk_map = event.data;
        worker.unref();
        // console.log(`Got map from worker: ${chunk_map.size}`);
        for (let [key, value] of chunk_map.entries()) {
            const existing = map.get(key);
            if (existing) {
              existing.min = Math.min(existing.min, value.min);
              existing.max = Math.max(existing.max, value.max);
              existing.sum   += value.sum;
              existing.count += value.count;
            } else {
              map.set(key, value);
            }
            // console.log(map);
        }
        Workers++;
        if ( Workers == chunks.length ) { process_map(); }
    };
}

function process_map() {
    let keys = Array.from(map.keys()).sort();
    for (let key of keys) {
        let val = map.get(key);
        console.log(key, ' : ', val.min, ', ', val.max, ', ', val.sum/val.count, ', ', val.count);
    }
    console.log('city number : ', keys.length)
    console.timeEnd();
}

async function process_chunk(start , end, threads) {
    // chunks.length = threads + 1
    let chunks = [ start ];
    
    const buf = new Buffer.alloc(100);

    const fp = await fsp.open(fname, 'r');
    const chunkSize = Math.floor((end - start + threads - 1) / threads);

    let offset = start;
    while (true) {
        offset += chunkSize;

        if (offset >= end) {
            chunks.push(end);
            break;
        }

        await fp.read(buf, 0, 100, offset);

        const nlPos = buf.indexOf(10);
        buf.fill(0);

        offset += nlPos + 1;
        chunks.push(offset);
    }
    await fp.close();
    return chunks;
}
