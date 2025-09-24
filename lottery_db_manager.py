from sqlalchemy import (
    create_engine,
    text,
    MetaData,
    Table,
    Column,
    String,
    BigInteger,
    Text,
    JSON,
    Date,
)
import json
from datetime import datetime
from lottery_models import LotteryDraw, PrizeGrade
import os

POSTGRES_URL = os.getenv("POSTGRES_URL")

TABLE_NAME = "double_color_ball"


class LotteryDBManager:
    def __init__(self):
        self.engine = create_engine(POSTGRES_URL)

    def create_table(self):
        """创建彩票数据表"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            code VARCHAR(50) PRIMARY KEY,
            week VARCHAR(10),
            red VARCHAR(50),
            blue VARCHAR(10),
            sales BIGINT,
            poolmoney BIGINT,
            content TEXT,
            prizegrades JSONB,
            date DATE
        );
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                print(f"表 '{TABLE_NAME}' 创建成功!")

                # 创建日期索引以提高查询性能
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS idx_lottery_date ON {TABLE_NAME}(date);"
                    )
                )
                conn.commit()
                print("日期索引创建成功!")

        except Exception as e:
            print(f"创建表时出错: {e}")

    def insert_data_from_json(self, json_file):
        """从JSON文件插入数据"""
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            inserted_count = 0
            with self.engine.connect() as conn:
                for item in data:
                    # 处理日期格式，去掉括号和星期
                    date_str = item.get("date", "")[:10]

                    insert_sql = f"""
                    INSERT INTO {TABLE_NAME} (code, week, red, blue, sales, poolmoney, content, prizegrades, date)
                    VALUES (:code, :week, :red, :blue, :sales, :poolmoney, :content, :prizegrades, :date)
                    ON CONFLICT (code) DO UPDATE SET
                        week = EXCLUDED.week,
                        red = EXCLUDED.red,
                        blue = EXCLUDED.blue,
                        sales = EXCLUDED.sales,
                        poolmoney = EXCLUDED.poolmoney,
                        content = EXCLUDED.content,
                        prizegrades = EXCLUDED.prizegrades,
                        date = EXCLUDED.date;
                    """

                    conn.execute(
                        text(insert_sql),
                        {
                            "code": item.get("code"),
                            "week": item.get("week"),
                            "red": item.get("red"),
                            "blue": item.get("blue"),
                            "sales": int(item.get("sales", 0)),
                            "poolmoney": int(item.get("poolmoney", 0)),
                            "content": item.get("content"),
                            "prizegrades": json.dumps(item.get("prizegrades", [])),
                            "date": date_str,
                        },
                    )
                    inserted_count += 1

                conn.commit()
                print(f"从 {json_file} 成功插入/更新 {inserted_count} 条记录")

        except Exception as e:
            print(f"插入数据时出错: {e}")

    def insert_all_data(self):
        """插入所有年份的数据"""
        years = range(2013, 2026)  # 2013-2025
        total_inserted = 0

        for year in years:
            json_file = f"./{year}.json"
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                with self.engine.connect() as conn:
                    for item in data:
                        date_str = item.get("date", "")[:10]

                        insert_sql = f"""
                        INSERT INTO {TABLE_NAME} (code, week, red, blue, sales, poolmoney, content, prizegrades, date)
                        VALUES (:code, :week, :red, :blue, :sales, :poolmoney, :content, :prizegrades, :date)
                        ON CONFLICT (code) DO UPDATE SET
                            week = EXCLUDED.week,
                            red = EXCLUDED.red,
                            blue = EXCLUDED.blue,
                            sales = EXCLUDED.sales,
                            poolmoney = EXCLUDED.poolmoney,
                            content = EXCLUDED.content,
                            prizegrades = EXCLUDED.prizegrades,
                            date = EXCLUDED.date;
                        """

                        conn.execute(
                            text(insert_sql),
                            {
                                "code": item.get("code"),
                                "week": item.get("week"),
                                "red": item.get("red"),
                                "blue": item.get("blue"),
                                "sales": int(item.get("sales", 0)),
                                "poolmoney": int(item.get("poolmoney", 0)),
                                "content": item.get("content"),
                                "prizegrades": json.dumps(item.get("prizegrades", [])),
                                "date": date_str,
                            },
                        )

                    conn.commit()
                    print(f"年份 {year}: 插入 {len(data)} 条记录")
                    total_inserted += len(data)

            except FileNotFoundError:
                print(f"文件 {json_file} 不存在，跳过")
            except Exception as e:
                print(f"处理年份 {year} 时出错: {e}")

        print(f"总共插入/更新 {total_inserted} 条记录")

    def query_all(self):
        """查询所有数据"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM {TABLE_NAME} ORDER BY date DESC")
                )
                rows = result.fetchall()

                print(f"总共 {len(rows)} 条记录:")
                for row in rows[:10]:  # 只显示前10条
                    lottery_draw = LotteryDraw.from_db_row(row)
                    print(
                        f"期号: {lottery_draw.code}, 日期: {lottery_draw.date}, "
                        f"红球: {lottery_draw.red}, 蓝球: {lottery_draw.blue}"
                    )

                if len(rows) > 10:
                    print(f"... 还有 {len(rows) - 10} 条记录")

        except Exception as e:
            print(f"查询数据时出错: {e}")

    def query_by_year(self, year):
        """按年份查询"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"SELECT * FROM {TABLE_NAME} WHERE EXTRACT(YEAR FROM date) = :year ORDER BY date"
                    ),
                    {"year": year},
                )
                rows = result.fetchall()

                print(f"{year} 年共有 {len(rows)} 条记录:")
                for row in rows:
                    lottery_draw = LotteryDraw.from_db_row(row)
                    print(
                        f"期号: {lottery_draw.code}, 日期: {lottery_draw.date}, "
                        f"红球: {lottery_draw.red}, 蓝球: {lottery_draw.blue}"
                    )

        except Exception as e:
            print(f"查询 {year} 年数据时出错: {e}")

    def query_by_code(self, code):
        """按期号查询"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM {TABLE_NAME} WHERE code = :code"),
                    {"code": code},
                )
                row = result.fetchone()

                if row:
                    lottery_draw = LotteryDraw.from_db_row(row)
                    print(lottery_draw)
                    lottery_draw.display_prizegrades()
                else:
                    print(f"未找到期号 {code}")

        except Exception as e:
            print(f"查询期号 {code} 时出错: {e}")

    def query_latest(self):
        """获取最新一期"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM {TABLE_NAME} ORDER BY date DESC LIMIT 1")
                )
                row = result.fetchone()
                if row:
                    lottery_draw = LotteryDraw.from_db_row(row)
                    print("=== 最新一期双色球 ===")
                    print(lottery_draw)
                    lottery_draw.display_prizegrades()
                    return lottery_draw
                return None
        except Exception as e:
            print(f"获取最新一期时出错: {e}")
            return None

    def get_statistics(self):
        """获取统计信息"""
        try:
            with self.engine.connect() as conn:
                # 总记录数
                total_result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
                total_count = total_result.fetchone()[0]

                # 年份统计
                year_result = conn.execute(
                    text(
                        f"""
                    SELECT EXTRACT(YEAR FROM date) as year, COUNT(*) 
                    FROM {TABLE_NAME} 
                    GROUP BY EXTRACT(YEAR FROM date) 
                    ORDER BY year
                """
                    )
                )
                year_stats = year_result.fetchall()

                # 销售总额
                sales_result = conn.execute(
                    text(f"SELECT SUM(sales) FROM {TABLE_NAME}")
                )
                total_sales = sales_result.fetchone()[0]

                print(f"=== 双色球数据统计 ===")
                print(f"总记录数: {total_count}")
                print(f"销售总额: {total_sales:,} 元")
                print(f"\n各年份统计:")
                for year, count in year_stats:
                    print(f"  {year}年: {count} 期")

        except Exception as e:
            print(f"获取统计信息时出错: {e}")


def main():
    db = LotteryDBManager()

    while True:
        print("\n=== 双色球数据库管理系统 ===")
        print("1. 创建数据表")
        print("2. 插入所有数据")
        print("3. 查询所有数据")
        print("4. 按年份查询")
        print("5. 按期号查询")
        print("6. 获取统计信息")
        print("7. 获取最新一期信息")
        print("8. 退出")

        choice = input("请选择操作 (1-7): ")

        if choice == "1":
            db.create_table()
        elif choice == "2":
            db.insert_all_data()
        elif choice == "3":
            db.query_all()
        elif choice == "4":
            year = input("请输入年份 (如: 2025): ")
            db.query_by_year(int(year))
        elif choice == "5":
            code = input("请输入期号 (如: 2025110): ")
            db.query_by_code(code)
        elif choice == "6":
            db.get_statistics()
        elif choice == "7":
            db.query_latest()
        elif choice == "8":
            print("再见!")
            break
        else:
            print("无效选择，请重试")


if __name__ == "__main__":
    main()
