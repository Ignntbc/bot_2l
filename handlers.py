import os
from aiogram.fsm.context import FSMContext
from aiogram import Bot, Dispatcher, types, F
from aiogram.dispatcher.router import Router
from aiogram.filters.command import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery,FSInputFile
from dotenv import load_dotenv
import asyncio
from typing import Optional, Literal

from buttons import reply_buttons, inline_buttons_list, inline_buttons_list_mistakes
from states import States
from db import DatabaseManager, AsyncDatabaseManager, con_ms, exec_query#, role_verification

load_dotenv()
bot = Bot(token=os.getenv('BOT_TOKEN'), parse_mode='HTML')

router = Router()

reply_buttons_list_bringer = ["Есть ошибка!", "Мои ошибки", "Список ошибок", "Выгрузить отчет", "Написать всем важную информацию", "Изменить статус"]
reply_buttons_list_admin = ["Отложенные", "Список ошибок", "Выгрузить отчет", "Написать всем важную информацию", "Изменить статус"]
roles_buttons_dict = {"Bringer" : reply_buttons_list_bringer, "Admin": reply_buttons_list_admin}
admin_buttons_list = ["Смотрю", "Требуется уточнение", "Есть решение", "Положил в Метеор", "Примеры больше не нужны"]
storage = MemoryStorage()
dp = Dispatcher()
# db_manager = AsyncDatabaseManager()
# main_labels_id = [str(i[0]) for i in db_manager.get_main_labels_id()]
async def main():
    global db_manager, main_labels_id
    db_manager = AsyncDatabaseManager()
    main_labels_id = [str(i[0]) for i in await db_manager.get_main_labels_id()]
asyncio.run(main())
async def send_document(chat_id: int, file_path: str, type: Literal["photo", "video", "document"], text: Optional[str] = None): 
    send_functions = {
        "photo": bot.send_photo,
        "video": bot.send_video,
        "document": bot.send_document,
    }
    send_func = send_functions.get(type)
    if send_func:
        if text:
            await send_func(chat_id, FSInputFile(file_path), caption=text)
        else:
            await send_func(chat_id, FSInputFile(file_path))
actions = {
    ('text_message', 'file_path', 'type_file'): db_manager.insert_description_with_file_and_text,
    ('text_message',): db_manager.insert_description_with_text,
    ('file_path', 'type_file'): db_manager.insert_description_with_file,
}

def data_answer_check(data, db_manager, admin_buttons_list):
    data["answer"] = db_manager.labels_examinations(data["task_id"])
    if data["answer"]:
        [data["button_list"].insert(-1, i) for i in admin_buttons_list if i not in data["button_list"]]
    else:
        [data["button_list"].remove(i) for i in admin_buttons_list if i in data["button_list"]]
    # return data

def process_labels(db_manager, data):
    labels_list_task_query = db_manager.get_labels_list_task_query(data["task_id"])
    standart_labels = db_manager.get_standart_labels()
    standart_labels_list = []
    labels_list_call_data = []
    labels_list_task = [labels_list_task_query[i][0] for i in range(len(labels_list_task_query))]
    print(labels_list_task)
    for i in range(len(standart_labels)):
        if standart_labels[i][0] in labels_list_task and "✅" not in standart_labels[i][1]:
            value = "✅" + standart_labels[i][1]
            label = "✅" + "label"
        else:
            value = standart_labels[i][1]
            label = "label"
        standart_labels_list.append(value)
        labels_list_call_data.append(str(standart_labels[i][0]) + label + str(data["task_id"]))
    labels_list_call_data.append("◀️ Назад")
    standart_labels_list.append("◀️ Назад") 
    data["labels_list_call_data"] = labels_list_call_data
    return standart_labels_list, labels_list_call_data


@router.message(Command(commands=["info"]))
async def process_info_command(message: Message):
    await message.answer('Информация о боте')


@router.message(Command(commands=["start"]))
async def process_start_command(message: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("role"):
        role = db_manager.role_verification(message.from_user.id)
        data["role"] = role
    if data.get("ms_id"):
        await bot.delete_message(chat_id=message.chat.id, message_id= data['ms_id'].message_id)    
    data["ms_id"] = await bot.send_message(message.from_user.id, "Главное меню" , reply_markup= reply_buttons(roles_buttons_dict[data["role"]]))
    await state.update_data(data)

@router.message(F.text =='Есть ошибка!')
async def buttton_have_mistake(message: Message, state: FSMContext):
    await state.set_state(States.create_task_name)
    data = await state.get_data()
    if data.get("ms_id"):
        await bot.delete_message(chat_id=message.chat.id, message_id= data['ms_id'].message_id)    
    data["ms_id"] = await bot.send_message(message.from_user.id, "Ошибке имя дай здесь. Без имени не найдешь ее после")
    await state.update_data(data)

@router.message(States.create_task_name)
async def start_create_rask(message: Message, state: FSMContext):
    data = await state.get_data()
    data["text_message"] = []
    data["message_info"] = []
    data["counter"] = 0
    if message.text:
        data["name_mistake"] = message.text
        if data.get("ms_id"):
            await bot.delete_message(chat_id=message.chat.id, message_id= data['ms_id'].message_id)   
        data["ms_id"] = await bot.send_message(message.from_user.id, '''Опиши ошибку и приложи, что принес...''')
        await state.set_state(States.create_task_info)
    else:
        await bot.send_message(message.from_user.id, "Вы не ввели имя ошибки")
    await state.update_data(data)

@router.message(States.create_task_info, F.text != "Ложная тревога", F.text != "Отправить ошибку")
async def start_create_rask(message: Message, state: FSMContext):
    data = await state.get_data()
    data["message_info"].append(message)
    await state.update_data(data)
    data["counter"] += 1
    await bot.send_message(message.from_user.id, f"Введено сообщений {data['counter']}", reply_markup= reply_buttons(["Ложная тревога", "Отправить ошибку"]))
    await bot.send_message(message.from_user.id, "Теги",reply_markup= inline_buttons_list(["⚡️⚡️⚡️Молния"]))
    await state.update_data(data)

@router.message(F.text == 'Ложная тревога', States.create_task_info)
async def button_false_alarm(message: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("role"):
        role = db_manager.role_verification(message.from_user.id)
        data["role"] = role
    if data.get("ms_id"):
        await bot.delete_message(chat_id=message.chat.id, message_id= data['ms_id'].message_id)   
    data["ms_id"] = await bot.send_message(message.from_user.id, "Главное меню" , reply_markup= reply_buttons(roles_buttons_dict[data["role"]]))
    await state.update_data(data)

@router.message(F.text == 'Отправить ошибку', States.create_task_info)
async def button_send_error(message: Message, state: FSMContext):
    data = await state.get_data()
    files_path = f'files/{message.from_user.id}'
    if not os.path.exists(files_path):
        os.makedirs(files_path)
    counter_message = 0
    for i in data["message_info"]:
        message_partions = {}
        message_partions["text_message"] = i.text or i.caption
        file_types = [("photo", i.photo, "jpg"), ("video", i.video, None), ("document", i.document, None)]
        for type_file, file, default_format in file_types:
            if file:
                if type_file == "photo":
                    max_size = max(file, key=lambda x: x.file_size)
                    file = max_size
                message_partions["file"] = file
                format = default_format or file.file_name.split(".")[-1]
                file_info = await bot.get_file(file.file_id)
                file_path = f"{files_path}/{file.file_id}.{format}"
                message_partions["file_path"] = file_path
                await bot.download_file(file_info.file_path, destination=file_path)
                message_partions["type_file"] = type_file
                break
        if counter_message == 0:
            db_manager.insert_mistake(data["name_mistake"], message.from_user.id)
            counter_message += 1

        for keys, action in actions.items():
            if all(message_partions.get(key) for key in keys):
                action(*(message_partions[key] for key in keys), message.from_user.id)
                break
        message_partions = {}

    if not data.get("role"):
        data["role"] = db_manager.role_verification(message.from_user.id)
    await state.update_data(data)
    await state.set_state(None)
    if data.get("ms_id"):
        await bot.delete_message(chat_id=message.chat.id, message_id= data['ms_id'].message_id)   
    data["ms_id"] = await bot.send_message(message.from_user.id, "Главное меню" , reply_markup= reply_buttons(roles_buttons_dict[data["role"]]))

@router.message(F.text == 'Мои ошибки')
async def buttton_my_mistakes(message: Message, state: FSMContext):
    await bot.send_message(message.from_user.id, "Вы выбрали кнопку 'Мои ошибки'" )
    


@router.message(F.text == 'Список ошибок')
async def buttton_list_of_errors(message: Message, state: FSMContext):
    data = await state.get_data()
    await state.set_state(States.my_mistakes)#{message.from_user.id}
    result = db_manager.get_mistakes_with_status()
    data["formatted_results"] = []
    data["call_data_buttons"] = []
    for item in result:
        formatted_date = item[3].strftime("%d.%m.%Y")
        formatted_string = f"{item[0]}{item[1]} {formatted_date} {item[2]}"
        data["formatted_results"].append(formatted_string)
        data["call_data_buttons"].append(str(item[1]))
        
    await state.set_state(None)
    await bot.send_message(message.from_user.id, f"{data['formatted_results']}",reply_markup= inline_buttons_list_mistakes(data["formatted_results"], data["call_data_buttons"]))
    await state.update_data(data)

@router.message(F.text == 'Выгрузить отчет')
async def button_download_report(message: Message):
    await bot.send_message(message.from_user.id, "Вы выбрали кнопку 'Выгрузить отчет'")

@router.message(F.text == 'Написать всем важную информацию')
async def button_write_information(message: Message):
    await bot.send_message(message.from_user.id, "Вы выбрали кнопку 'Написать всем важную информацию'" )

@router.message(F.text == 'Изменить статус')
async def button_change_status(message: Message):
    await bot.send_message(message.from_user.id, "Добро пожаловать, сегодня ты на страже Галактики. Сегодня моя миссия: (выберите)",
                           reply_markup= inline_buttons_list(["✉️ Приношу ошибки", "⚙️ Чиню ошибки"]))
    
@router.callback_query(lambda c: c.data in ('✉️ Приношу ошибки',"⚙️ Чиню ошибки") )
async def callback_handler_status(call: CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    if call.data == "✉️ Приношу ошибки":
        data["role"] = "Bringer"
        db_manager.role_changer(call.message.chat.id, 2)
    elif call.data == "⚙️ Чиню ошибки":
        data["role"] = "Admin"
        db_manager.role_changer(call.message.chat.id, 1)
    await state.set_state(None)
    await state.update_data(data)
    await bot.send_message(call.message.chat.id, "Главное меню" , reply_markup= reply_buttons(roles_buttons_dict[data["role"]]))

@router.callback_query()
async def callback_handler_other(call: CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    base_buttons_list = ["Уведомлять", "Все сообщения файлом"]
    if data.get("call_data_buttons"):
        if call.data in data["call_data_buttons"]:
            data["task_id"] = call.data
            result =  db_manager.get_mistake_details(call.data)
            #справочник по возвращаемому ответу: [0] - mistake_id , [1] - mistake_name, [2] - user_first_name
            #[3] - user sername, [4] - user tg tag, [5] - text message, [6] - file_path
            # [7] - file_type, [8] - first massrge, [9] - status_id , [10] - bts_link
            # [11] - created_at, [12] - mistake_status_name
            labels = db_manager.get_mistake_labels(call.data)
            labels_names = [labels[i][1] for i in range(len(labels))]
            first_message = f"<b>{result[0][0]}. {result[0][1]}</b>\n<i>{result[0][2]} {result[0][3]}</i> @{result[0][4]}"
            changelog_message = f"<b>{result[0][0]}. {result[0][1]}</b>"+ f"\n<code>{', '.join(labels_names)}</code>" + f"\n<b>Автор:</b> {result[0][2]} {result[0][3]} @{result[0][4]}"#добавить строку с mistakes_labels
            await bot.send_message(call.message.chat.id, first_message)
            for item in result:
                if item[9]:    
                    changelog_message += f"\n<code>{item[11].strftime('%Y-%m-%d %H:%M:%S')[:item[11].strftime('%Y-%m-%d %H:%M:%S').rfind(':')]}</code> {item[12]} ({item[2]} {item[3]} @{item[4]})"
                if item[8]:
                    if item[5] and item[6]:
                        print(item[5], item[6])
                        await send_document(call.message.chat.id, item[6], item[7], item[5])
                    elif item[5]:
                        await bot.send_message(call.message.chat.id, text=item[5])
                    elif item[6]:
                        await send_document(call.message.chat.id, file_path= item[6], type= item[7])
                else:
                    text = f"<code>{item[2]} {item[3]}</code> {item[11][:item[11].rfind(':')]} @{item[4]}" + item[5]
                    if item[5] and item[6]:
                        await send_document(call.message.chat.id, file_path=item[6], type=item[7],text= text)
                    elif item[5]:
                        await bot.send_message(call.message.chat.id, text=text)
                    elif item[6]:
                        await send_document(call.message.chat.id, file_path= item[6], type= item[7])
            
            button_list = base_buttons_list
            for label in labels:
                if label[2]:
                    button_list.extend(admin_buttons_list)
            data["changelog_message"] = changelog_message
            if not data.get("role"):
                data["role"] = db_manager.role_verification(call.from_user.id)
            if data["role"] == "Bringer":
                button_list.extend(["Спасибо, решено", "Ещё пример", "◀️Список"])
            elif data["role"] == "Admin":
                button_list.extend(["Теги", "◀️Список"])
            data["button_list"] = button_list
            if not data.get("answer"):
                data_answer_check(data, db_manager, admin_buttons_list)
            data["changelog_message_id"] = await bot.send_message(call.message.chat.id,changelog_message,reply_markup= inline_buttons_list(data["button_list"]))# Возможно удалить ченджлог меседж если не используется
    if call.data == "◀️ Назад":
        data_answer_check(data, db_manager, admin_buttons_list)
        labels = db_manager.get_mistake_labels(data["task_id"])
        labels_names = [labels[i][1] for i in range(len(labels))]
        result =  db_manager.get_mistake_details(data["task_id"])
        changelog_message = f"<b>{result[0][0]}. {result[0][1]}</b>"+ f"\n<code>{', '.join(labels_names)}</code>" + f"\n<b>Автор:</b> {result[0][2]} {result[0][3]} @{result[0][4]}"
        for item in result:
                if item[9]:    
                    changelog_message += f"\n<code>{item[11].strftime('%Y-%m-%d %H:%M:%S')[:item[11].strftime('%Y-%m-%d %H:%M:%S').rfind(':')]}</code> {item[12]} ({item[2]} {item[3]} @{item[4]})"
        await bot.edit_message_text(text=changelog_message,chat_id= call.message.chat.id, reply_markup= inline_buttons_list(data["button_list"]),  message_id= data["changelog_message_id"].message_id)
    elif call.data == "Теги":
        standart_labels_list, labels_list_call_data = process_labels(db_manager, data)
        await bot.edit_message_text(text = "Выберите метки",chat_id = call.message.chat.id,reply_markup= inline_buttons_list_mistakes(standart_labels_list, labels_list_call_data), message_id= data["changelog_message_id"].message_id)

    if data.get("labels_list_call_data"):
        if call.data in data["labels_list_call_data"] and call.data != "◀️ Назад":
            if '✅' in call.data:
                db_manager.delete_label(call.data[call.data.find("label") + 5:], call.data[:call.data.find("✅")])
            else:
                print(call.data[:call.data.find("label")], f"{main_labels_id} - main_labels_id")
                if call.data[:call.data.find("label")] in main_labels_id:
                    print("есть заход в цикл")
                    db_manager.insert_main_label(call.data[call.data.find("label") + 5:], call.data[:call.data.find("label")])
                else:
                    db_manager.insert_label(call.data[call.data.find("label") + 5:], call.data[:call.data.find("label")])   
            standart_labels_list, labels_list_call_data = process_labels(db_manager, data)
            await bot.edit_message_text(text="Выберите метки",chat_id = call.from_user.id,  reply_markup=inline_buttons_list_mistakes(standart_labels_list, labels_list_call_data), message_id= data["changelog_message_id"].message_id)
    
    await state.update_data(data)

# async def main():


#     # dp.include_routers(
#     #     alarm_questions.router,
#     #     registration_questions.router,
#     #     user_commands.router,
#     # )

#     await bot.delete_webhook(drop_pending_updates=True)
#     await dp.start_polling(bot)



if __name__ == '__main__':
    async def start():
        dp.include_routers(router)
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)

    asyncio.run(start())