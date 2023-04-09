import './App.css';
import { Header } from './components/Header';
import Layout, { Content, Header as HeaderWrapper } from 'antd/es/layout/layout';

function App() {
  return (
    <Layout>
        <HeaderWrapper>
            <Header></Header>
            <div className='wrapper'></div>
        </HeaderWrapper>
        <Content></Content>
    </Layout>
  );
}

export default App;
