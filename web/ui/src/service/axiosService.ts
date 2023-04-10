import { Axios } from 'axios';
import { rowData } from '../types/types';
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
    async getAll(from: number, to: number): Promise<rowData[]> {
        try {
            const response = await this.axios.get('/all');
            return JSON.parse(response.data);
        } catch (err) {
            this.errorHandler(err);
        }
        return [];
    }
}
const axiosService = new AxiosService();

export default axiosService;