export type typeOfRow = 'string' | 'list' | 'hash' | 'set' | 'zset';
type valueOfStringRow = string;
type valueOfListRow = string[];
type valueOfSetRow = string[];
type valueOfHashRow = {
    [key in string]: string
};
type valueOfZsetRow = {
    [key in string]: number
}
export type valueOfRow<T extends typeOfRow> = T extends 'string' ? valueOfStringRow : 
                                              T extends 'list' ? valueOfListRow :
                                              T extends 'hash' ? valueOfHashRow :
                                              T extends 'set' ? valueOfSetRow :
                                              T extends 'zset' ? valueOfZsetRow :
                                              never;
export interface RowData<T extends typeOfRow>{
    key: string,
    type: T,
    ttl: number,
    value: valueOfRow<T>
}
export type RowDataAny = RowData<typeOfRow>;