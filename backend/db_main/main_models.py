from sqlalchemy import Table,Column,MetaData,String,Integer

metadata_obj = MetaData()

main_table = Table(
    "ludice_main_table",
    metadata_obj,
    Column("user_id",String,primary_key = True,unique = True),
    Column("balance",Integer),
    Column("games_count",Integer),
    Column("wins_count",Integer)
)