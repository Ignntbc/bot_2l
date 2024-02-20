import re
import os
from dotenv import load_dotenv
from typing import Optional, Literal
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import asyncio
import aioodbc

load_dotenv()


class AsyncDatabaseManager:
    def __init__(self, db: Optional[str] = None):
        if db is None:
            con_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SERVER_MSSM')};UID={os.getenv('UID_MSSM')};PWD={os.getenv('PWD_MSSM')};TrustServerCertificate=yes"
        else:
            con_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SERVER_MSSM')};DATABASE={db};UID={os.getenv('UID_MSSM')};PWD={os.getenv('PWD_MSSM')};"
        self.con_str = con_str

    async def exec_query(self, query, mode='query'):
        result = ''
        async with aioodbc.create_pool(self.con_str) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if mode == 'query':
                        await cur.execute(query)
                    elif mode == 'dict':
                        await cur.execute(query)
                        result_cur = await cur.fetchall()
                        result = [result_cur[x] for x in range(len(result_cur))]   
                    elif mode == 'value':
                        await cur.execute(query)
                        result_cur = await cur.fetchall()
                        for i in range(len(result_cur)):
                            if i == (len(result_cur))-1:
                                result += str(result_cur[i][0])
                            else :
                                result += str(result_cur[i][0]) +', '
                    elif mode == 'list':
                        await cur.execute(query)
                        list_forms_fetchall = await cur.fetchall()
                        result = [x for fetch in list_forms_fetchall for x in fetch]
        return result
    
    async def labels_examinations(self, mistake_id: str):
        main_labels_query = """
        SELECT id
        FROM [mistake-labels]
        WHERE is_main = 1
        """
        main_labels_set = set(await self.exec_query(main_labels_query, mode='list'))

        mistake_labels_query = f"""
        SELECT mistake_label_id
        FROM [labels-lists]
        WHERE mistake_id = {mistake_id}
        """
        mistake_labels_list = await self.exec_query(mistake_labels_query, mode='list')

        return any(element in main_labels_set for element in mistake_labels_list)

    async def get_main_labels_id(self):
        main_labels_query = """
        SELECT id
        FROM [mistake-labels]
        WHERE is_main = 1
        """
        return await self.exec_query(main_labels_query, mode='dict')
    
    async def role_verification(self, id: int):
        role_query = f"""
        SELECT TOP(1) role_name
        FROM users u
        LEFT JOIN roles r on u.role_id = r.id
        WHERE u.tg_id = {id}
        """
        return await self.exec_query(role_query, mode='value')

    async def insert_mistake(self, mistake_name: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[mistakes] 
        (mistake_name, mistake_author_id, status_id, create_datetime, on_3l) 
        VALUES 
        (N'{mistake_name}', (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}), 2, GETDATE(), 0)
        """
        await self.exec_query(query)

    async def insert_description_with_file_and_text(self, text_message: str, file_path: str, type_file: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[description] 
        (number_mistake, text_message, file_path, created_at, first_message, user_id,  file_type) 
        VALUES 
        ((SELECT MAX(id) FROM [dbo].[mistakes]), N'{text_message}', '{file_path}', GETDATE(), 1, 
        (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}), '{type_file}')
        """
        await self.exec_query(query)

    async def insert_description_with_text(self, text_message: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[description] 
        (number_mistake, text_message, created_at, first_message, user_id) 
        VALUES 
        ((SELECT MAX(id) FROM [dbo].[mistakes]), N'{text_message}', GETDATE(), 1, 
        (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}))
        """
        await self.exec_query(query)

    async def insert_description_with_file(self, file_path: str, type_file: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[description] 
        (number_mistake, file_path, created_at, first_message, user_id,  file_type) 
        VALUES 
        ((SELECT MAX(id) FROM [dbo].[mistakes]), '{file_path}', GETDATE(), 1, 
        (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}), '{type_file}')
        """
        await self.exec_query(query)

    async def get_labels_list_task_query(self, task_id):
        query = f'''select mistake_label_id
                    from [labels-lists]
                    where mistake_id = {task_id}'''
        return await self.exec_query(query, mode='dict')

    async def get_standart_labels(self):
        query = f'''select [m-l].id, [m-l].name
                    from [mistake-labels] [m-l]'''
        return await self.exec_query(query, mode='dict')

    async def get_mistake_details(self, mistake_id):
        query = f'''SELECT m.id, m.mistake_name, u.first_name AS owner_name, u.sername AS owner_sername, u.tag_tg AS tag_owner,
                    d.text_message, d.file_path, d.file_type, d.first_message, d.status_id, d.bts_link, d.created_at, ms.name 
                    FROM mistakes m
                    LEFT JOIN description d ON m.id = d.number_mistake
                    LEFT JOIN users u ON m.mistake_author_id = u.id
                    LEFT JOIN [mistake-statuses] ms ON d.status_id = ms.id
                    WHERE m.id = {mistake_id} '''
        return await self.exec_query(query, mode='dict')

    async def get_mistake_labels(self, mistake_id):
        query = f'''select [m-l].id, [m-l].name, [m-l].is_main
                    from [labels-lists] [l-l]
                    left join [mistake-labels] [m-l] on [l-l].mistake_label_id = [m-l].id
                    where [l-l].mistake_id = {mistake_id} '''
        return await self.exec_query(query, mode='dict')

    async def delete_label(self, mistake_id, label_id):
        query = f"""
        DELETE FROM [labels-lists] 
        WHERE mistake_id = '{mistake_id}' 
        and mistake_label_id = '{label_id}'
        """
        await self.exec_query(query)

    async def role_changer(self, chat_id: int, role_id: int):
        query = f"""
        UPDATE users
        set role_id = {role_id}
        where tg_id = {chat_id}
        """
        await self.exec_query(query)

    async def get_mistakes_with_status(self):
        query = f"""
        SELECT ms.smile, m.id, mistake_name, create_datetime
        FROM mistakes m
        LEFT JOIN  [mistake-statuses] ms on m.status_id = ms.id
        """
        return await self.exec_query(query, mode='dict')

    async def insert_label(self, mistake_id, label_id):
        query = f"""
        INSERT INTO [dbo].[labels-lists] 
        (mistake_id, mistake_label_id) 
        VALUES ('{mistake_id}','{label_id}')
        """
        await self.exec_query(query)

    async def insert_main_label(self, mistake_id, label_id):
        delete_query = f"""
        DELETE FROM [labels-lists]
        WHERE mistake_id = {mistake_id} and 
        mistake_label_id in (SELECT id FROM [mistake-labels] WHERE is_main = 1)
        """
        insert_query = f"""
        INSERT INTO [dbo].[labels-lists] 
        (mistake_id, mistake_label_id) 
        VALUES ('{mistake_id}','{label_id}')
        """
        await self.exec_query(delete_query)
        await self.exec_query(insert_query)
    


class DatabaseManager:
    def __init__(self, db: Optional[str] = None, closing_connection: Optional[bool] = True):
        if db is None:
            con_str = f"mssql+pymssql://{os.getenv('UID_MSSM')}:{os.getenv('PWD_MSSM')}@{os.getenv('SERVER_MSSM')}/"
        else:
            con_str = f"mssql+pymssql://{os.getenv('UID_MSSM')}:{os.getenv('PWD_MSSM')}@{os.getenv('SERVER_MSSM')}/{db}"

        if closing_connection is True:
            self.engine = create_engine(con_str, poolclass=NullPool, isolation_level='AUTOCOMMIT')
        else:
            self.engine = create_engine(con_str, pool_size=20, isolation_level='AUTOCOMMIT')

    def exec_query(self, query, mode='query'):
        connection = self.engine.raw_connection()
        result = ''
        try:
            cursor = connection.cursor()
            if mode == 'query':
                cursor.execute(query)
            elif mode == 'dict':
                cursor.execute(query)
                result_cur = cursor.fetchall()
                result = [result_cur[x] for x in range(len(result_cur))]   
            elif mode == 'value':
                cursor.execute(query)
                result_cur = cursor.fetchall()
                for i in range(len(result_cur)):
                    if i == (len(result_cur))-1:
                        result += str(result_cur[i][0])
                    else :
                        result += result_cur[i][0] +', '
            elif mode == 'list':
                cursor.execute(query)
                list_forms_fetchall:list = cursor.fetchall()
                result = [x for fetch in list_forms_fetchall for x in fetch]
            cursor.close()
            connection.commit()
        finally:
            connection.close()
        return result
    def labels_examinations(self, mistake_id: str):
        main_labels_query = """
        SELECT id
        FROM [mistake-labels]
        WHERE is_main = 1
        """
        main_labels_set = set(self.exec_query(main_labels_query, mode='list'))

        mistake_labels_query = f"""
        SELECT mistake_label_id
        FROM [labels-lists]
        WHERE mistake_id = {mistake_id}
        """
        mistake_labels_list = self.exec_query(mistake_labels_query, mode='list')

        return any(element in main_labels_set for element in mistake_labels_list)
    
    def get_main_labels_id(self):
        main_labels_query = """
        SELECT id
        FROM [mistake-labels]
        WHERE is_main = 1
        """
        return self.exec_query(main_labels_query, mode='dict')

    def role_verification(self, id: int):
        role_query = f"""
        SELECT TOP(1) role_name
        FROM users u
        LEFT JOIN roles r on u.role_id = r.id
        WHERE u.tg_id = {id}
        """
        return self.exec_query(role_query, mode='value')
    def insert_mistake(self, mistake_name: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[mistakes] 
        (mistake_name, mistake_author_id, status_id, create_datetime, on_3l) 
        VALUES 
        (N'{mistake_name}', (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}), 2, GETDATE(), 0)
        """
        self.exec_query(query)

    def insert_description_with_file_and_text(self, text_message: str, file_path: str, type_file: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[description] 
        (number_mistake, text_message, file_path, created_at, first_message, user_id,  file_type) 
        VALUES 
        ((SELECT MAX(id) FROM [dbo].[mistakes]), N'{text_message}', '{file_path}', GETDATE(), 1, 
        (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}), '{type_file}')
        """
        self.exec_query(query)

    def insert_description_with_text(self, text_message: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[description] 
        (number_mistake, text_message, created_at, first_message, user_id) 
        VALUES 
        ((SELECT MAX(id) FROM [dbo].[mistakes]), N'{text_message}', GETDATE(), 1, 
        (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}))
        """
        self.exec_query(query)

    def insert_description_with_file(self, file_path: str, type_file: str, user_id: int):
        query = f"""
        INSERT INTO [dbo].[description] 
        (number_mistake, file_path, created_at, first_message, user_id,  file_type) 
        VALUES 
        ((SELECT MAX(id) FROM [dbo].[mistakes]), '{file_path}', GETDATE(), 1, 
        (SELECT TOP(1) id FROM users WHERE tg_id = {user_id}), '{type_file}')
        """
        self.exec_query(query)

    def get_labels_list_task_query(self, task_id):
        result = exec_query(con_ms(),f'''select mistake_label_id
                                          from [labels-lists]
                                          where mistake_id = {task_id}''', mode='dict')
        return result

    def get_standart_labels(self):
        result = exec_query(con_ms(),f'''select [m-l].id, [m-l].name
                                          from [mistake-labels] [m-l]''', mode='dict')
        return result
    def get_mistake_details(self, mistake_id):
        result = exec_query(con_ms(),f'''SELECT m.id, m.mistake_name, u.first_name AS owner_name, u.sername AS owner_sername, u.tag_tg AS tag_owner,
                                          d.text_message, d.file_path, d.file_type, d.first_message, d.status_id, d.bts_link, d.created_at, ms.name 
                                          FROM mistakes m
                                          LEFT JOIN description d ON m.id = d.number_mistake
                                          LEFT JOIN users u ON m.mistake_author_id = u.id
                                          LEFT JOIN [mistake-statuses] ms ON d.status_id = ms.id
                                          WHERE m.id = {mistake_id} ''', mode='dict')
        return result
    
    def get_mistake_labels(self, mistake_id):
        result = exec_query(con_ms(),f'''select [m-l].id, [m-l].name, [m-l].is_main
                                        from [labels-lists] [l-l]
                                        left join [mistake-labels] [m-l] on [l-l].mistake_label_id = [m-l].id
                                        where [l-l].mistake_id = {mistake_id} ''', mode='dict')
        return result
    
    def delete_label(self, mistake_id, label_id):
        exec_query(con_ms(),f'''DELETE FROM [labels-lists] 
                                WHERE mistake_id = '{mistake_id}' 
                                and mistake_label_id = '{label_id}' ''')

    def role_changer(self, chat_id: int, role_id: int):
        exec_query(con_ms(),f'''UPDATE users
                                set role_id = {role_id}
                                where tg_id = {chat_id} ''')

    def get_mistakes_with_status(self):
        result = exec_query(con_ms(),f'''SELECT ms.smile, m.id, mistake_name, create_datetime
                                          FROM mistakes m
                                          LEFT JOIN  [mistake-statuses] ms on m.status_id = ms.id
                                          ''', mode='dict')
        return result
    
    def insert_label(self, mistake_id, label_id):
        exec_query(con_ms(),f'''INSERT INTO [dbo].[labels-lists] 
                                (mistake_id, mistake_label_id) 
                                VALUES ('{mistake_id}','{label_id}')''')
    def insert_main_label(self, mistake_id, label_id):
        exec_query(con_ms(),f'''DELETE FROM [labels-lists]
                                WHERE mistake_id = {mistake_id} and 
                                mistake_label_id in (SELECT id FROM [mistake-labels] WHERE is_main = 1);
                                INSERT INTO [dbo].[labels-lists] 
                                (mistake_id, mistake_label_id) 
                                VALUES ('{mistake_id}','{label_id}')''')

def con_ms(db: Optional[str] = None, closing_connection: Optional[bool] = True):

    if db== None:
        con_str = f"mssql+pymssql://{os.getenv('UID_MSSM')}:{os.getenv('PWD_MSSM')}@{os.getenv('SERVER_MSSM')}/"
    else:
        con_str = f"mssql+pymssql://{os.getenv('UID_MSSM')}:{os.getenv('PWD_MSSM')}@{os.getenv('SERVER_MSSM')}/{db}"

    if closing_connection is True:
        engine = create_engine(con_str, poolclass=NullPool, isolation_level='AUTOCOMMIT')
    else:
        engine = create_engine(con_str, pool_size=20, isolation_level='AUTOCOMMIT')
    return engine

def exec_query(con, query, mode='query'):
    connection = con.raw_connection()
    result = ''
    try:
        cursor = connection.cursor()
        if mode == 'query':
            cursor.execute(query)
        elif mode == 'dict':
            cursor.execute(query)
            result_cur = cursor.fetchall()
            result = [result_cur[x] for x in range(len(result_cur))]   
        elif mode == 'value':
            cursor.execute(query)
            result_cur = cursor.fetchall()
            # result = [result_cur[x][0] for x in range(len(result_cur))]   
            for i in range(len(result_cur)):
                if i == (len(result_cur))-1:
                    result += str(result_cur[i][0])
                else :
                    result += result_cur[i][0] +', '
        elif mode == 'list':
            cursor.execute(query)
            list_forms_fetchall:list = cursor.fetchall()
            result = [x for fetch in list_forms_fetchall for x in fetch]
        cursor.close()
        connection.commit()
    finally:
        connection.close()
    return result