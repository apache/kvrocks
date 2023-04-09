import { useCallback } from 'react';
import logo from '../logo.svg';
import styles from './Header.module.css';
import { Menu } from 'antd';
import { ItemType } from 'antd/es/menu/hooks/useItems';
const menuItems: ItemType[] = [
    {
        key: 'data',
        label: 'Data',
    },
    {
        key: 'state',
        label: 'State',
    },
]
export const Header = () => {
    const onLogoClick = useCallback(() => {
        console.log('logo clicked')
    },[])
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
                theme="dark" 
                mode="horizontal" 
                items={menuItems}
                defaultSelectedKeys={['data']}
                style={{width: '100%', marginLeft: '50px'}}
            ></Menu>
        </div>
    )
}