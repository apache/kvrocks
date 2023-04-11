import { Axios } from 'axios';
import { BaseRow, rowData } from '../types/types';
class AxiosService {
    private axios: Axios;
    constructor() {
        this.axios = new Axios({
            baseURL: 'http://localhost:8888'
            // baseURL: window.location.origin
        });
    }
    private errorHandler(err: any) {
        console.error('Api error',err);
    }
    private async post(url: string, data: object) {
        try {
            const {data:result}=await this.axios.post(url, JSON.stringify(data), {
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            return result;
        } catch (err) {
            this.errorHandler(err);
        }
    }
    async getAll(from: number, to: number): Promise<{data:rowData[], totalCount: number}> {
        try {
            const response = await this.axios.get('/all', {
                params: {
                    from,
                    to
                }
            });
            return JSON.parse(response.data);
        } catch (err) {
            this.errorHandler(err);
        }
        return {
            data: [],
            totalCount: 0
        };
    }
    async create(data: BaseRow) {
        await this.post('/create', data);
        return;
    }
}
const axiosService = new AxiosService();

export default axiosService;