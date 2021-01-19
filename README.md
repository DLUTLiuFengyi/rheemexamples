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

### 注意
#### Spark in rheem

Rheem使用的spark环境需要是1.x，因为其内部使用了一个1.x版本才有的函数

```java
sparkContext.clearJars();
```
但打包时要把spark.version改成2.x，因为1.x版本在这里打成的jar包无法执行（找不到主类）

虽然jar包内依赖是2.x版本，但只要spark集群版本是1.x就能顺利执行jar包

#### jar包版本冲突

打包之后执行时还遇到“错误：找不到或无法加载jar包”的话，先检查Main-class是否已经声明

再检查jar包依赖有没有版本冲突，此时需要把src/main/下新建的jars文件夹删去，重新用IDEA生成，再重新build jar包

#### Hadoop in rheem

rheem的包里已经导入了hadoop相关依赖包，因此pom中无需再单独导入hadoop相关依赖包