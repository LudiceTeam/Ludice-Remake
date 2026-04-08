from sqlalchemy import Table,Column,MetaData,String,Integer,ARRAY

metadata_obj = MetaData()


game_table = Table(
    "game_table",
    metadata_obj,
    Column("game_id",String,primary_key = True,unique=True),
    Column("player_1",String),
    Column("player_2",String),
    Column("bet",Integer),
    Column("winner",String)
)