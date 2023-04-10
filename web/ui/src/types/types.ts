interface baseRow{
    key: string,
    type: string,
    value: any
}
interface stringRow extends baseRow{
    type: 'string',
    value: string
}
interface listRow extends baseRow{
    type: 'list',
    value: string[]
}
interface hashRow extends baseRow{
    type: 'hash',
    value: {
        [key in string]: string
    }
}
interface setRow extends baseRow{
    type: 'set',
    value: string[]
}
export type rowData= stringRow|listRow|hashRow|setRow
