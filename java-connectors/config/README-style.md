# Lenses Code style
In order to be consistent with our Code Style of our Java sources we have three configurations that we use: 

- IntelliJ IDEA config 
- Eclipse Code Formatter config 
- Eclipse format config 

Below we'll quickly describe which config helps us in which scenario.

## IntelliJ IDEA 

This configuration can be easily loaded if you're using JetBrains' IntelliJ IDEA.

All you need to do is to go: **Settings> Editor > Code Style > Import (scheme) > IntelliJ IDEA code style (xml)** and load the file (`config/Lenses_IDEA.xml`). However for full compatibility we advise to pair it with [Adapter for Eclipse Code Formatter](https://plugins.jetbrains.com/plugin/6546-adapter-for-eclipse-code-formatter)  plugin then importing its config by going through **Eclipse Code Formatter config** section.

## Eclipse Code Formatter config

If you use [Adapter for Eclipse Code Formatter](https://plugins.jetbrains.com/plugin/6546-adapter-for-eclipse-code-formatter) plugin in your IntelliJ you can import this config by putting `config/eclipseCodeFormatter.xml` file into your `.idea` folder then checking if it loaded correctly in **Settings > Adapter for Eclipse Code Formatter** section (it should load Eclipse format config from section below). Using this configuration you should be able to get the same results no matter whether you use *Reformat Code* option in IntelliJ or using Gradle. 

## Eclipse format config 

If you're using Eclipse IDE or you want to use our Spotless Gradle plugin with Eclipse config you can import `config/Lenses_eclipse.xml` into your IDE. If you build using our Gradle plugins, it will be automatically imported (then you can use e.g. `spotlessApply` command).

 