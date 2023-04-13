export interface BaseRow{
    key: string,
    type: string,
    ttl: number,
    value: any
}
export interface StringRow extends BaseRow{
    type: 'string',
    value: string
}
export interface ListRow extends BaseRow{
    type: 'list',
    value: string[]
}
export interface HashRow extends BaseRow{
    type: 'hash',
    value: {
        [key in string]: string
    }
}
export interface SetRow extends BaseRow{
    type: 'set',
    value: string[]
}
export type rowData= StringRow|ListRow|HashRow|SetRow
