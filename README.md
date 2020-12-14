### IDEA将maven项目打包成jar包

* File -> Project Structure

* Artifacts -> 点击+号 -> Jar -> From modules with dependencies

* Create JAR from Modules 界面

  Module 

  选择要打包的子模块

  Main Class 

  选择入口main函数

  JAR files from libraries 

  选择extract to the target JAR

  Derictory for ... MF 

  将默认路径最后的java改成jars（即在src/main/下新建一个jars文件夹，存放...MF文件）

  OK

* 回到Artifacts界面，jar包的默认名字和输出路径不用改 -> Apply -> OK
* Build -> Build Artifacts -> 选择要打包的Artifacts，build
* 根据Artifacts界面里jar包的输出路径找到生成的jar包