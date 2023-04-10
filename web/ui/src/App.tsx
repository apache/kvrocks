import './App.css';
import { Header } from './components/Header';
import Layout, { Content, Header as HeaderWrapper } from 'antd/es/layout/layout';
import { useRoutes } from 'react-router-dom';
import { router } from './router';


function App() {
    const routerElement = useRoutes(router);
    return (
        <Layout>
            <HeaderWrapper style={{backgroundColor: 'white'}}>
                <Header></Header>
                <div className='wrapper'></div>
            </HeaderWrapper>
            <Content>
                {
                    routerElement
                }
            </Content>
        </Layout>
    );
}

export default App;
