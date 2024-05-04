// import * as fsp from 'node:fs/promises';
import * as fs from 'node:fs';

self.onmessage = (event) => {
    let data = event.data;
    let map = process_file( data.start, data.end, data.threads );
    postMessage(map);
};

function process_file(start, end, threads) {
    let offset = start;
    let chunks = [ start ];

    const buf = new Buffer.alloc(100);

    const chunkSize = Math.floor((end - start + threads - 1) / threads);

    const fname = '/tmp/measurements.txt';
    const fp = fs.openSync(fname, 'r');
    while (true) {
        offset += chunkSize;

        if (offset >= end) {
            chunks.push(end);
            break;
        }

        fs.readSync(fp, buf, 0, 100, offset);

        const nlPos = buf.indexOf(10);
        buf.fill(0);

        offset += nlPos + 1;
        chunks.push(offset);
    }
    // console.log( chunks );

    const map = new Map();

    for (let i = 1; i < chunks.length; i++) {
        let length = chunks[i] - chunks[i-1] - 1;

        const buf2 = new Buffer.alloc(length);
        fs.readSync(fp, buf2, 0, length, chunks[i-1]);

        let lines = buf2.toString().split('\n');
        for (let line of lines) {
            let dd = line.split(';');
            let city = dd[0];
            let temp = Number(dd[1]);
            let key = map.get(city);
            if (key) {
                key.min = key.min < temp ? key.min : temp;
                key.max = key.max > temp ? key.max : temp;
                key.sum += temp;
                key.count++;
            } else {
                map.set(city, { min: temp,  max: temp, sum: temp, count: 1 });
            }
        }
    }
    return map;
}
