/// @brief 运行全部测试并返回失败计数。
int RunTests();

/// @brief 测试程序入口，根据失败计数返回进程退出码。
int main() { return RunTests() == 0 ? 0 : 1; }
