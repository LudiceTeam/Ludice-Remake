from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv 
import os
from backend.db_main.main_models import metadata_obj,main_table
import logging

logger = logging.getLogger(__name__)


load_dotenv()


async_engine = create_async_engine(
    f"postgresql+asyncpg://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@localhost:5432/ludice_main_database",
    pool_size=20,           # Размер пула соединений
    max_overflow=50,        # Максимальное количество соединений
    pool_recycle=3600,      # Пересоздавать соединения каждый час
    pool_pre_ping=True,     # Проверять соединение перед использованием
    echo=False
)




AsyncSessionLocal = sessionmaker(
    async_engine, 
    class_=AsyncSession,
    expire_on_commit=False
)

async def drop_table():
    async with async_engine.begin() as conn:
        await conn.run_sync(metadata_obj.drop_all)

async def create_table():
    async with async_engine.begin() as conn:
        await conn.run_sync(metadata_obj.create_all)



async def create_user(user_id:str) -> bool:
    async with AsyncSession(async_engine) as conn:
        async with conn.begin():
            try:
                stmt = insert(main_table).values(
                    user_id = user_id,
                    balance = 20,
                    wins_count = 0,
                    games_count = 0,
                ).on_conflict_do_nothing(
                    index_elements=[main_table.c.user_id]
                )
                result = await conn.execute(stmt)

                if result.rowcount == 0:
                    return False
                return True
                
            except Exception:
                logger.exception("MAIN SQL ERROR")
                return False

async def upper_balance(user_id:str,amount:int):
    async with AsyncSession(async_engine) as conn:
        async with conn.begin():
            try:
                stmt = main_table.update().where(main_table.c.user_id == user_id).values(
                    balance = main_table.c.balance + amount
                )
                await conn.execute(stmt)
            except Exception:
                logger.exception("MAIN SQL ERROR")
                return         

async def lower_balance(user_id:str,amount:int):
    async with AsyncSession(async_engine) as conn:
        async with conn.begin():
            try:
                stmt = main_table.update().where(main_table.c.user_id == user_id).values(
                    balance = main_table.c.balance - amount
                )
                await conn.execute(stmt)
            except Exception:
                logger.exception("MAIN SQL ERROR")
                return   

async def get_user_balance(user_id:str):
    async with AsyncSession(async_engine) as conn:
        try:
            pass
        except Exception:
            logger.exception("MAIN SQL ERROR")
            return   
    