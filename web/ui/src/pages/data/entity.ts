import { BaseRow, rowData } from '../../types/types';

export interface RowType {
    key: string,
    value: string,
    type: rowData['type'],
    ttl: number,
}

export class KvRow implements RowType {
    rawValue: rowData['value'];
    key: string;
    value: string;
    type: rowData['type'];
    ttl: number;
    constructor(row?: rowData) {
        if(!row) {
            this.key = '';
            this.rawValue = '';
            this.value = '';
            this.type = 'string';
            this.ttl = -1;
            return;
        }
        this.key = row.key;
        this.type = row.type;
        this.rawValue = row.value;
        this.ttl = row.ttl;
        switch (row.type) {
        case 'string':
            this.value = row.value;
            break;
        case 'list':
        case 'set':
            this.value = row.value.join(',');
            break;
        case 'hash':
            this.value = JSON.stringify(row.value);
            break;
        default:
            this.value = '';
            break;
        }
    }
    generateDefaultRawValueForType() {
        switch (this.type) {
        case 'string':
            this.rawValue = '';
            break;
        case 'list':
        case 'set':
            this.rawValue = [];
            break;
        case 'hash':
            this.rawValue = {};
            break;
        default:
            this.value = '';
            break;
        }
    }
    getPostBody(): BaseRow {
        const result = {
            key: this.key,
            type: this.type,
            value: this.rawValue,
            ttl: this.ttl
        };
        return result;
    }
}

export class kvDataTable {
    rows: KvRow[];
    constructor(data: rowData[]) {
        this.rows = data.map(r => new KvRow(r));
    }
}