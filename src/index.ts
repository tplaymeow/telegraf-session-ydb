import { SessionStore } from "telegraf";
import { Column, Driver, TableDescription, TypedValues, Types } from "ydb-sdk";

export interface YdbOpts<Session> {
  /**
   * The driver instance used to connect to the database.
   */
  driver: Driver;

  /**
   * Options for configuring the driver behavior.
   */
  driverOptions?: DriverOpts;

  /**
   * Options for configuring table-related operations.
   */
  tableOptions?: TableOpts;

  /**
   * Options for configuring parsing operations.
   */
  parsingOptions?: ParsingOptions<Session>;
}

export interface DriverOpts {
  /**
   * Enables or disables the readiness check for the driver.
   * If set to `false`, the `readyCheckTimeout` parameter is ignored.
   * @default true
   */
  enableReadyCheck?: boolean;

  /**
   * Timeout (in milliseconds) for the readiness check.
   * Only used if `enableReadyCheck` is set to `true`.
   * @default 10000
   */
  readyCheckTimeout?: number;
}

export interface TableOpts {
  /**
   * Indicates whether the table should be created if it does not exist.
   * @default false
   */
  shouldCreateTable?: boolean;

  /**
   * The name of the table to be used or created.
   * @default "telegraf-sessions"
   */
  tableName?: string;

  /**
   * The name of the column used as the key.
   * @default "key"
   */
  keyColumnName?: string;

  /**
   * The data type of the data stored in the key column.
   * Supported types: "String", "Utf8".
   * @default "Utf8"
   */
  keyColumnType?: "String" | "Utf8";

  /**
   * The name of the column used for storing session data.
   * @default "session"
   */
  sessionColumnName?: string;

  /**
   * The data type of the session data stored in the session column.
   * Supported types: "String", "Utf8", "Json".
   * @default "Json"
   */
  sessionColumnType?: "String" | "Utf8" | "Json";
}

export interface ParsingOptions<Session> {
  /**
   * Parses a session string into the corresponding session data.
   *
   * @param sessionString - The session data as a string.
   * @returns The parsed session data.
   * @default JSON.parse
   */
  parse?: (sessionString: string) => Session;

  /**
   * Stringifies session data into a string format.
   *
   * @param session - The session data to stringify.
   * @returns The session data represented as a string.
   * @default JSON.stringify
   */
  stringify?: (session: Session) => string;
}

const defaultDriverOptions = {
  enableReadyCheck: false,
  readyCheckTimeout: 10000,
};

const defaultTableOptions = {
  shouldCreateTable: false,
  tableName: "telegraf-sessions",
  keyColumnName: "key",
  keyColumnType: "Utf8",
  sessionColumnName: "session",
  sessionColumnType: "Json",
};

const defaultParsingOptions = {
  parse: JSON.parse,
  stringify: JSON.stringify,
};

export function Ydb<Session>(options: YdbOpts<Session>): SessionStore<Session> {
  const driverOptions = mergeOptions(
    defaultDriverOptions,
    options.driverOptions,
  );
  const tableOptions = mergeOptions(defaultTableOptions, options.tableOptions);
  const parsingOptions = mergeOptions(
    defaultParsingOptions,
    options.parsingOptions,
  );

  const ready = async () => {
    if (driverOptions.enableReadyCheck) {
      if (!(await options.driver.ready(driverOptions.readyCheckTimeout))) {
        throw new Error(`Failed to initialize driver`);
      }
    }
  };

  const getSessionQuery = `
    DECLARE $key AS ${tableOptions.keyColumnType};
    SELECT \`${tableOptions.sessionColumnName}\` FROM \`${tableOptions.tableName}\`
    WHERE \`${tableOptions.keyColumnName}\` = $key
  `;

  const setSessionQuery = `
    DECLARE $key AS ${tableOptions.keyColumnType};
    DECLARE $session AS ${tableOptions.sessionColumnType}?;
    UPSERT INTO \`${tableOptions.tableName}\` (\`${tableOptions.keyColumnName}\`, \`${tableOptions.sessionColumnName}\`)
    VALUES ($key, $session);
  `;

  const deleteSessionQuery = `
    DECLARE $key AS ${tableOptions.keyColumnType};
    DELETE FROM \`${tableOptions.tableName}\` WHERE \`${tableOptions.keyColumnName}\` = $key;
  `;

  let keyColumnType;
  switch (tableOptions.keyColumnType) {
    case "String":
      keyColumnType = Types.TEXT;
      break;
    case "Utf8":
      keyColumnType = Types.UTF8;
      break;
    default:
      throw new Error(`Unsupported key type: ${tableOptions.keyColumnType}`);
  }

  let sessionColumnType;
  switch (tableOptions.sessionColumnType) {
    case "String":
      sessionColumnType = Types.TEXT;
      break;
    case "Utf8":
      sessionColumnType = Types.UTF8;
      break;
    case "Json":
      sessionColumnType = Types.JSON;
      break;
    default:
      throw new Error(
        `Unsupported session type: ${tableOptions.sessionColumnType}`,
      );
  }

  const createTable = tableOptions.shouldCreateTable
    ? options.driver.tableClient.withSessionRetry(async (session) => {
        await session.createTable(
          tableOptions.tableName,
          new TableDescription()
            .withColumn(new Column(tableOptions.keyColumnName, keyColumnType))
            .withColumn(
              new Column(tableOptions.sessionColumnName, sessionColumnType),
            ),
        );
      })
    : Promise.resolve();

  return {
    async get(key: string): Promise<Session | undefined> {
      await ready();
      await createTable;

      const dbResult = await options.driver.tableClient.withSessionRetry(
        async (session) => {
          return await session.executeQuery(getSessionQuery, {
            $key: TypedValues.fromNative(keyColumnType, key),
          });
        },
      );

      const stringSession = dbResult.resultSets
        .at(0)
        ?.rows?.at(0)
        ?.items?.at(0)?.textValue;
      if (!stringSession) {
        return undefined;
      }

      return parsingOptions.parse(stringSession);
    },
    async set(key: string, value: Session): Promise<void> {
      await ready();
      await createTable;

      await options.driver.tableClient.withSessionRetry(async (session) => {
        return await session.executeQuery(setSessionQuery, {
          $key: TypedValues.fromNative(keyColumnType, key),
          $session: TypedValues.optional(
            TypedValues.fromNative(
              sessionColumnType,
              parsingOptions.stringify(value),
            ),
          ),
        });
      });
    },
    async delete(key: string): Promise<void> {
      await ready();
      await createTable;

      await options.driver.tableClient.withSessionRetry(async (session) => {
        return await session.executeQuery(deleteSessionQuery, {
          $key: TypedValues.fromNative(keyColumnType, key),
        });
      });
    },
  };
}

function mergeOptions<T>(defaultOpts: T, userOpts?: Partial<T>): T {
  return { ...defaultOpts, ...userOpts };
}
