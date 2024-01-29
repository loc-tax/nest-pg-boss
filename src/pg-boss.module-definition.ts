import { ConfigurableModuleBuilder } from "@nestjs/common";
import { PGBossModuleOptions } from "./interfaces";

const {
  ConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN,
  OPTIONS_TYPE,
  ASYNC_OPTIONS_TYPE,
} = new ConfigurableModuleBuilder<PGBossModuleOptions>()
  .setClassMethodName("forRoot")
  .build();

type ASYNC_OPTIONS_TYPE_WITH_NAME = typeof ASYNC_OPTIONS_TYPE;

export {
  ConfigurableModuleClass,
  MODULE_OPTIONS_TOKEN,
  OPTIONS_TYPE,
  ASYNC_OPTIONS_TYPE_WITH_NAME as ASYNC_OPTIONS_TYPE,
};
