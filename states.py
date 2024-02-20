from aiogram.filters.state import StatesGroup, State

class States (StatesGroup):
    create_task_name = State()
    create_task_info = State()
    my_mistakes = State()
