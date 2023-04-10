import { useEffect, useState } from 'react';
import axiosService from '../../service/axiosService';
import { rowData } from '../../types/types';

export const DataPage = function() {
    const [data, setData] = useState<rowData[]>([]);
    useEffect(() => {
        (async () => {
            const data = await axiosService.getAll(0,10);
            setData(data);
            console.log(data);
        })();
    }, []);
    return (
        <div>
            data: {JSON.stringify(data)}
        </div>
    );
};