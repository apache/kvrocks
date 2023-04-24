import { useCallback, useEffect, useState } from 'react';
import axiosService from '../../service/axiosService';
import { InfoType } from '../../types/types';
import { Collapse, Table } from 'antd';
import { useLocalstorage } from '../../common/hooks';

export const InfoPage = function() {
    const [info, setInfo] = useState<InfoType>({});
    const [defaultExpand, setDefaultExpand] = useLocalstorage('infoDefaultExpand', ['Server', 'Clients']);
    useEffect(() => {
        fetch();
    }, []);
    const fetch = useCallback(async () => {
        setInfo(
            await axiosService.getInfo()
        );
    },[]);
    return (
        <div>
            <Collapse defaultActiveKey={defaultExpand} onChange={e => setDefaultExpand(e as string[])}>
                {
                    (function() {
                        const resultDom = [];
                        for (const key in info) {
                            if (Object.prototype.hasOwnProperty.call(info, key)) {
                                const details = info[key];
                                const detailsList = [];
                                for (const detailsKey in details) {
                                    if (Object.prototype.hasOwnProperty.call(details, detailsKey)) {
                                        detailsList.push({
                                            key: detailsKey,
                                            value: details[detailsKey]
                                        });
                                    }
                                }
                                resultDom.push((<Collapse.Panel header={key} key={key}>
                                    <Table
                                        dataSource={detailsList}
                                        columns={[
                                            {
                                                width: 200,
                                                dataIndex: 'key'
                                            },
                                            {
                                                width: 200,
                                                dataIndex: 'value'
                                            }
                                        ]}
                                        size='small'
                                        showHeader={false}
                                        pagination={false}
                                    ></Table>
                                </Collapse.Panel>));
                            }
                        }
                        return resultDom;
                    })()
                }
            </Collapse>
        </div>
    );
};