from sqlalchemy import select,update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv 
import os
from backend.game_db.game_core import metadata_obj,game_table
import logging
import uuid

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



async def create_or_find_game(user_id:str,bet:int) -> dict:
    async with AsyncSession(async_engine) as conn:
        async with conn.begin():
            try:
                stmt = select(
                    game_table.c.game_id,
                    game_table.c.player_1,
                    
                ).where(
                    game_table.c.player_2.is_(None),
                    game_table.c.player_1 != user_id,
                    game_table.c.bet == bet
                ).with_for_update(skip_locked=True).limit(1)

                res = await conn.execute(stmt)

                data = res.mappings().first()

                if data is not None:
                    stmt_update = update(game_table).where(game_table.c.game_id == data["game_id"]).values(
                        player_2 = user_id
                    )

                    await conn.execute(stmt_update)
                    
                    return {
                        "found" : True,
                        "game_id" : data["game_id"]
                    }
                else:
                    new_game_id = str(uuid.uuid4())
                    stmt_insert = insert(game_table).values(
                        game_id = new_game_id,
                        player_1 = user_id,
                        player_2 = None,
                        bet = bet,
                        winner = None
                    )
                    await conn.execute(stmt_insert)
                    return {
                        "found" : False,
                        "game_id" : new_game_id
                    }

            except Exception:
                logger.exception("GAME SQL ERROR")
                return {}
            
async def delete_game(game_id:str):
    async with AsyncSession(async_engine) as conn:
        async with conn.begin():
            try:
                stmt = game_table.delete().where(game_table.c.game_id == game_id)
                await conn.execute(stmt)
            except Exception:
                logger.exception("GAME SQL ERROR")
                return
            
async def write_winner_to_game(game_id:str,winner:str):
    async with AsyncSession(async_engine) as conn:
        async with conn.begin():
            try:
                stmt = update(game_table).where(
                    game_table.c.game_id == game_id
                ).values(
                    winner = winner
                )
                await conn.execute(stmt)
            except Exception:
                logger.exception("GAME SQL ERROR")
                return