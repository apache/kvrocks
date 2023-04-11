export interface BaseRow{
    key: string,
    type: string,
    ttl: number,
    value: any
}
interface StringRow extends BaseRow{
    type: 'string',
    value: string
}
interface ListRow extends BaseRow{
    type: 'list',
    value: string[]
}
interface HashRow extends BaseRow{
    type: 'hash',
    value: {
        [key in string]: string
    }
}
interface SetRow extends BaseRow{
    type: 'set',
    value: string[]
}
export type rowData= StringRow|ListRow|HashRow|SetRow
