import { Axios } from 'axios';
import { BaseRow, rowData } from '../types/types';
import { notification } from 'antd';
class AxiosService {
    private axios: Axios;
    constructor() {
        this.axios = new Axios({
            baseURL: 'http://localhost:8888'
            // baseURL: window.location.origin
        });
    }
    private errorHandler(err: any) {
        let errMsg = 'Unknown error';
        if(typeof err == 'string') {
            errMsg = err;
        }else if(typeof err?.response?.data == 'string') {
            errMsg = err.response.data;
        }else if(typeof err?.message == 'string') {
            errMsg = err.message;
        }
        notification.error({
            message: 'Request error',
            description: errMsg,
            placement: 'bottomRight'
        });
        console.error('Api error',err);
    }
    private async post(url: string, data: object) {
        try {
            const {data:result}=await this.axios.post(url, JSON.stringify(data), {
                headers: {
                    'Content-Type': 'application/json'
                },
                validateStatus: code => code == 200
            });
            return result;
        } catch (err) {
            this.errorHandler(err);
            return false;
        }
    }
    private async get(url: string, params:object) {
        try {
            const response = await this.axios.get(url, {params});
            return JSON.parse(response.data);
        } catch (err) {
            this.errorHandler(err);
            return false;
        }
    }
    async getAll(from: number, to: number): Promise<{data:rowData[], totalCount: number} | false> {
        return await this.get('/all',{from,to});
    }
    async create(data: BaseRow): Promise<string | false> {
        return await this.post('/create', data);
    }
}
const axiosService = new AxiosService();

export default axiosService;