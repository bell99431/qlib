from joblib import Parallel, parallel_backend, delayed
import abc

class MyClass(abc.ABC):
    def __init__(self):
        pass
    
    def _process_data(self, data):
        # 处理数据的逻辑
        return data
    
    def process_data_parallel(self, data_list):
        # 定义并发处理数据的函数
        def process_data(data):
            return self._process_data(data)
        
        # 使用 'loky' 后端来避免尝试序列化线程锁对象
        with parallel_backend('loky'):
            results = Parallel(n_jobs=5)(delayed(process_data)(data) for data in data_list)
        
        return results

# 示例用法
my_instance = MyClass()
data_list = [1,2,3,4,5,6]  # 数据列表
results = my_instance.process_data_parallel(data_list)
print(results)
