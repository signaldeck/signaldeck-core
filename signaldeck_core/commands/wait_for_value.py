from signaldeck_sdk import Cmd, Command
from signaldeck_sdk.value_provider import ValueProvider
import asyncio,logging

class WaitForValue(Command):
    def __init__(self,vP:ValueProvider,sleep=20):
        self.vP=vP
        self.sleep=sleep
        self.logger= logging.getLogger("waitForValue")
        super().__init__("waitFor","Waits for condition. Usage: waitFor <fieldName> <operator> <value>. Operators: =,>,>=,<,<=. FieldName is the fieldName in ValueProvider")

    def matches(self,curVal, operator, targetValue):
        if curVal is None:
            return False
        curVal = float(curVal)
        targetValue= float(targetValue)
    
        if operator == "=":
            return curVal == targetValue
        if operator == ">":
            return curVal > targetValue
        if operator == ">=":
            return curVal >= targetValue
        if operator == "<":
            return curVal < targetValue
        if operator == "<=":
            return curVal <= targetValue

    async def run(self,fieldName: str,operator: str, value: float, cmdRes=None,stopEvent=None):
        self.logger.info(f'Wait for {fieldName} {operator} {value}')
        if cmdRes is not None:
            cmdRes.appendState(self,msg=f'Wait for {fieldName} {operator} {value}')
        curValue=self.vP.getValue(fieldName)
        while not self.matches(curValue,operator,value) and not stopEvent.is_set():
            await asyncio.sleep(self.sleep)
            curValue=self.vP.getValue(fieldName)
            self.logger.debug(f'Check value: {curValue}')
        if stopEvent.is_set():
            cmdRes.appendState(self,msg=f'Stop erkannt')
            return
        if cmdRes is not None:
            cmdRes.appendState(self,msg=f'Matched condition! {fieldName}={curValue}')
        self.logger.info(f'Matched condition! {fieldName}={curValue}')