import { useCallback } from 'react';
import logo from '../logo.svg';
import styles from './Header.module.css';
import { Dropdown, Menu, Space, Typography } from 'antd';
import { DownOutlined } from '@ant-design/icons';
import { ItemType } from 'antd/es/menu/hooks/useItems';
import type { MenuClickEventHandler } from 'rc-menu/lib/interface';
import { useNavigate, useLocation } from 'react-router-dom';

const menuItems: ItemType[] = [
    {
        key: '/data',
        label: 'Data',
    },
    {
        key: '/info',
        label: 'Info',
    },
];
const languageItems = [
    {
        key: 'en-us',
        label: 'English',
    },
    {
        key: 'zh-cn',
        label: '中文',
    },
];
export const Header = () => {
    const navigate = useNavigate();
    const {pathname: currentRoutePath} = useLocation();
    const onLogoClick = useCallback(() => {
        navigate('/');
    },[navigate]);
    const onMenuClick:MenuClickEventHandler = useCallback(e => {
        navigate(e.key);
    },[navigate]);
    return (
        <div className={styles.wrapper}>
            <div className={styles.left} onClick={onLogoClick}>
                <div className={styles.leftWrapperFlex}>
                    <img className={styles.logo} alt='logo' src={logo}></img>
                    <div className={styles.title}>
                        Kvrocks
                    </div>
                </div>
            </div>
            <Menu 
                theme="light" 
                mode="horizontal" 
                items={menuItems}
                selectedKeys={[currentRoutePath]}
                className={styles.menu}
                onClick={onMenuClick}
            ></Menu>
            <div className={styles.right}>
                <Dropdown menu={{
                    items: languageItems,
                    selectable: true,
                    defaultSelectedKeys: ['en-us']
                }}>
                    <Typography.Link>
                        <Space>
                            Language
                            <DownOutlined />
                        </Space>
                    </Typography.Link>
                </Dropdown>
            </div>
        </div>
    );
};