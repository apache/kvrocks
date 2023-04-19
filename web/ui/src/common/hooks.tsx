import { useState } from 'react';

export function useLocalstorage<T extends number | string | object>(key: string, initValue: T): [value: T, setValue: (value: T) => any] {
    function transformToString(value: T): string {
        let valueStr = '';
        if (typeof value == 'string') {
            valueStr = value;
        } else if (typeof value == 'number') {
            valueStr = `${value}`;
        } else if (typeof value == 'object') {
            valueStr = JSON.stringify(value);
        }
        return valueStr;
    }
    function transformToT(value: string): T {
        try {
            if (typeof initValue == 'string') {
                return value as T;
            } else if (typeof initValue == 'number') {
                return parseInt(value) as T;
            } else if (typeof initValue == 'object') {
                return JSON.parse(value);
            }
        } catch (error) {
            console.error(error);
        }
        return value as T;
    }
    const setLocalstorageValue = (newValue: T) => {
        setValue(newValue);
        localStorage.setItem(key, transformToString(newValue));
    };
    const [value, setValue] = useState<T>(() => {
        const storedValue: T = transformToT(localStorage.getItem(key) || '');
        if(!storedValue) {
            localStorage.setItem(key, transformToString(initValue));
        }
        return storedValue || initValue;
    });
    return [value, setLocalstorageValue];
}