from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
# from aiogram.types.web_app_info import WebAppInfo



def reply_buttons(*args):
    
    row1, row2, row3 = [], [], []

    for index, item in enumerate(args[0]):
        button = KeyboardButton(text=item, callback_data=item)
        if index < 3:
            row1.append(button)
        elif index < 5:
            row2.append(button)
        else:
            row3.append(button)
    keyboard_buttons = ReplyKeyboardMarkup(resize_keyboard=True,
                                           keyboard=[row1, row2, row3])
    return keyboard_buttons

# def inline_buttons(*args):
#     list_button_name = [[*args]]
#     buttons_list = []
#     for item in list_button_name:
#         l = []
#         for i in item:
#             l.append(InlineKeyboardButton(text=i, callback_data=i))
#         buttons_list.append(l)

#     keyboard_inline_buttons = InlineKeyboardMarkup(inline_keyboard=buttons_list, row_width=1)
#     return keyboard_inline_buttons

def inline_buttons_list(list):
    buttons_list = []
    for i in list:
        button = InlineKeyboardButton(text=i, callback_data=i)
        buttons_list.append([button]) 
    keyboard_inline_buttons = InlineKeyboardMarkup(inline_keyboard=buttons_list)
    return keyboard_inline_buttons

def inline_buttons_list_mistakes(list, calldata_list):
    buttons_list = []
    for i in range(len(list)):
        button = InlineKeyboardButton(text=list[i], callback_data=str(calldata_list[i]))
        buttons_list.append([button]) 
    keyboard_inline_buttons = InlineKeyboardMarkup(inline_keyboard=buttons_list)
    return keyboard_inline_buttons