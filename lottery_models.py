from dataclasses import dataclass
from typing import List, Dict, Any
import json

@dataclass
class PrizeGrade:
    """奖项等级信息"""
    type: int
    typenum: str
    typemoney: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PrizeGrade':
        return cls(
            type=data.get('type', 0),
            typenum=data.get('typenum', ''),
            typemoney=data.get('typemoney', '')
        )

@dataclass
class LotteryDraw:
    """双色球开奖数据"""
    code: str                    # 期号
    week: str                    # 星期
    red: str                     # 红球号码
    blue: str                    # 蓝球号码
    sales: int                   # 销售额
    poolmoney: int               # 奖池金额
    content: str                 # 中奖详情
    prizegrades: List[PrizeGrade] # 奖项等级信息
    date: str                    # 开奖日期
    
    @classmethod
    def from_db_row(cls, row) -> 'LotteryDraw':
        """从数据库行创建LotteryDraw对象"""
        prizegrades_data = row[7] if row[7] else []
        # 如果是字符串，则解析为JSON；如果已经是列表，则直接使用
        if isinstance(prizegrades_data, str):
            prizegrades_data = json.loads(prizegrades_data)
        
        prizegrades = [PrizeGrade.from_dict(pg) for pg in prizegrades_data]
        
        return cls(
            code=row[0],
            week=row[1],
            red=row[2],
            blue=row[3],
            sales=row[4],
            poolmoney=row[5],
            content=row[6],
            prizegrades=prizegrades,
            date=str(row[8])
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'week': self.week,
            'red': self.red,
            'blue': self.blue,
            'sales': self.sales,
            'poolmoney': self.poolmoney,
            'content': self.content,
            'prizegrades': [pg.__dict__ for pg in self.prizegrades],
            'date': self.date
        }
    
    def __str__(self) -> str:
        """字符串表示"""
        return f"""双色球第 {self.code} 期
日期: {self.date} ({self.week})
红球: {self.red}
蓝球: {self.blue}
销售额: {self.sales:,} 元
奖池: {self.poolmoney:,} 元
中奖详情: {self.content}
奖项等级: {len(self.prizegrades)} 个等级"""
    
    def display_prizegrades(self) -> None:
        """显示奖项等级详情"""
        print("奖项等级详情:")
        for pg in self.prizegrades:
            print(f"  {pg.type}等奖: {pg.typenum}注, 单注奖金: {pg.typemoney}元")
