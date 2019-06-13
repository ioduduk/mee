# -*- coding: utf-8 -*-

"""
modules的接口
"""

from zope.interface import Interface, Attribute

STATUS_INITIAL = 'INITIAL'
STATUS_INCRE_UPDATING = 'INCRE_UPDATING'
STATUS_FULL_UPDATING = 'FULL_UPDATING'

class IStatus(Interface):
    """
    任务状态module层接口
    """

    code = Attribute("""status code""")

    config = Attribute("""key of current configuration""")

    nextConfig = Attribute("""key of new configuration""")

    tmpConfig = Attribute("""key of temp configuration""")

    def sync():
        """
        从远程DB(redis或者mysql)获取任务状态
        """
    
    def create():
        """
        创建任务状态
        """

    def update():
        """
        更新任务状态
        """

    def delete():
        """
        删除任务状态
        """

    def cloneStatus(otherStatus):
        """
        复制任务状态
        """

class IStatusConfig(Interface):
    """
    任务配置
    """
    key = Attribute("""key of configuration""")

    handlerConfig = Attribute("""handler config""")

    kafkaGroupId = Attribute("""consumer group id of kafka""")

    esIndexSuffix = Attribute("""index suffix of elasticsearch""")

    handled = Attribute("""the hanlder config has been used for handling or not""")

    def syncByKey():
        """
        从远程DB(redis或者mysql)同步任务配置
        """

    def set():
        """
        更新/设置任务配置
        """

    def init():
        """
        重置/清空任务配置
        """

    def delete():
        """
        删除任务配置
        """

class IHandlerConfig(Interface):
    """
    同步数据到ES的处理器配置
    """

class IHandlerConfigList(Interface):
    """
    处理器配置列表。一个配置列表会解析为ES中一个index中的一个type
    """

class IHandlerConfigItem(Interface):
    """
    处理器配置项。一个配置列表会包含多个项。
    """

class IHandler(Interface):
    """
    处理同步事宜的处理器接口
    """
    def syncFromMySQL():
        """
        从mysql中获取数据进行全量更新
        """

    def syncFromBinlog(binlogEvent):
        """
        增量更新：将binlog event中的数据同步到ES中
        """







