
import { DataPage } from './pages/data/DataPage';
import { InfoPage } from './pages/info/InfoPage';
import { RouteObject, Navigate } from 'react-router-dom';
export const router:RouteObject[] = [
    {
        path: '/',
        element: <Navigate to="/data"></Navigate>
    },
    {
        path: '/data',
        element: <DataPage></DataPage>,
    },
    {
        path: '/info',
        element: <InfoPage></InfoPage>,
    }
];