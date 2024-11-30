import asyncio
from typing import Optional, List
from pydantic import BaseModel
from fastapi import FastAPI, Depends, HTTPException
from sqlmodel import Field, Session, SQLModel, create_engine, select
from starlette.concurrency import run_in_threadpool
from parser import find_name_and_price

app = FastAPI()

sqlite_file_name = "prices.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=True)


# Модель цены и названия кружки
class Prices(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    price: int


# Модель для входящих данных
class ItemCreate(BaseModel):
    name: str
    price: int


# создание таблиц и базы данных
def create_db_and_tables():
    """
    Создает таблицы и базу данных.
    """
    SQLModel.metadata.create_all(engine)


create_db_and_tables()


# URL сайта и путь до категории
base_url = "https://www.maxidom.ru/"
start_path = "catalog/kruzhki/"


# Фоновая задача
async def background_parser_async(session_factory):
    """
    Фоновая задача для парсинга данных и сохранения их в базе данных.

    Args:
        session_factory (Callable): Фабрика для создания сессий базы данных.
    """
    while True:
        try:
            print("Начало парсинга...")
            elements = await run_in_threadpool(find_name_and_price, base_url, start_path)
            if elements:
                with session_factory() as session:
                    for name, price in elements:
                        # Создаем запрос на проверку существования продукта
                        query = select(Prices).where(Prices.name == name, Prices.price == price)
                        product_exists = session.exec(query).first()
                        if not product_exists:
                            item = Prices(name=name, price=price)
                            session.add(item)
                        else:
                            print(f"Пропускаем дубликат {name} - {price}")
                    session.commit()
                print(f"Добавлено {len(elements)} элементов в базу данных.")
            else:
                print("Элементы не найдены")
        except Exception as e:
            print(f"Ошибка при фоновом парсинге: {e}")
        await asyncio.sleep(12 * 60 * 60)  # 12 часов


# Функция для получения сессии базы данных
def get_session():
    """
    Функция для получения сессии базы данных.

    Yields:
        Session: Сессия базы данных.
    """
    with Session(engine) as session:
        yield session


# получение сессии
SessionDep = Depends(get_session)


# старт приложения
@app.on_event("startup")
async def startup_event():
    """
    Событие, выполняемое при старте приложения.
    Запускает фоновую задачу для парсинга данных.
    """
    asyncio.create_task(background_parser_async(lambda: Session(engine)))


# возвращает все элементы из базы данных
@app.get("/prices/", response_model=List[Prices])
async def read_prices(session: Session = SessionDep):
    """
    Возвращает все элементы из базы данных.

    Args:
        session (Session): Сессия базы данных.

    Returns:
        List[Prices]: Список всех элементов из базы данных.
    """
    return session.exec(select(Prices)).all()


# возвращает все элементы из базы данных со смещением и ограничением на вывод
@app.get("/prices-with-offset/", response_model=List[Prices])
async def read_prices_offset_limit(offset: int, limit: int, session: Session = SessionDep):
    """
    Возвращает все элементы из базы данных со смещением и ограничением на вывод.

    Args:
        offset (int): Смещение.
        limit (int): Ограничение на количество элементов.
        session (Session): Сессия базы данных.

    Returns:
        List[Prices]: Список элементов из базы данных с учетом смещения и ограничения.
    """
    return session.exec(select(Prices).offset(offset).limit(limit)).all()


# возвращает элемент по ID
@app.get("/prices/{item_id}", response_model=Prices)
async def read_item(item_id: int, session: Session = SessionDep):
    """
    Возвращает элемент по ID.

    Args:
        item_id (int): ID элемента.
        session (Session): Сессия базы данных.

    Returns:
        Prices: Элемент из базы данных.

    Raises:
        HTTPException: Если элемент не найден.
    """
    item = session.get(Prices, item_id)
    if item:
        return item
    raise HTTPException(status_code=404, detail="Такого элемента не существует. Кружка не найдена.")


@app.put("/prices/{item_id}", response_model=Prices)
async def update_item(item_id: int, new_item: ItemCreate, session: Session = SessionDep):
    """
    Изменяет элемент по ID.

    Args:
        item_id (int): ID элемента.
        item (ItemCreate): Данные измененного элемента.
        session (Session): Сессия базы данных.

    Returns:
        Prices: Обновленный элемент.

    Raises:
        HTTPException: Если элемент не найден.
    """
    db_item = session.get(Prices, item_id)
    if db_item:
        db_item.name = new_item.name
        db_item.price = new_item.price
        session.commit()
        session.refresh(db_item)  # Обновляем объект в сессии
        return db_item
    raise HTTPException(status_code=404, detail="Такого элемента не существует. Кружка не найдена.")


# добавляет новый элемент в базу данных
@app.post("/prices/create", response_model=Prices)
async def create_item(new_item: ItemCreate, session: Session = SessionDep):
    """
    Добавляет новый элемент в базу данных.

    Args:
        item (ItemCreate): Данные нового элемента.
        session (Session): Сессия базы данных.

    Returns:
        Prices: Добавленный элемент.
    """
    item = Prices(name=new_item.name, price=new_item.price)
    session.add(item)
    session.commit()
    session.refresh(item)
    return item


# удаляет элемент по ID
@app.delete("/prices/{item_id}", response_model=dict)
async def delete_item(item_id: int, session: Session = SessionDep):
    """
    Удаляет элемент по ID.

    Args:
        item_id (int): ID элемента.
        session (Session): Сессия базы данных.

    Returns:
        dict: Сообщение об успешном удалении.

    Raises:
        HTTPException: Если элемент не найден.
    """
    db_item = session.get(Prices, item_id)
    if db_item:
        session.delete(db_item)
        session.commit()
        return {"status": "ok"}
    raise HTTPException(status_code=404, detail="Такого элемента не существует. Кружка не найдена.")

# Запуск приложения
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
