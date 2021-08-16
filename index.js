const dotenv = require("dotenv");
dotenv.config();
const mysql = require("mysql2/promise");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const SnowflakeId = require("snowflake-id");
const snowflake = new SnowflakeId.default({
  mid: 1,
  offset: (2021 - 1970)* 31536000 * 1000
});
const actionTypeMap = {
  "FOREGROUND": 1,
  "BACKGROUND": 2,
  "FIST": 3,
  "TOOL": 3,
  "NONE": 3
};

const inputFilePath = path.resolve(process.cwd(), process.argv[2] || "384390");
if (!fs.existsSync(inputFilePath)) return console.log("You must specify a valid input file path.");
let itemData;

try {
  itemData = fs.readFileSync(inputFilePath, { encoding: "utf8" });
  itemData = JSON.parse(itemData);
  if (!itemData.item_dat_version || !itemData.items || itemData.items.length < 11000) throw new Error("Invalid data file.");
} catch (err) {
  return console.log(`An error occured when parsing the item data:\n${err}`);
}

let items = itemData.items;
let textureFiles = [];
items = items.filter(item => {
  if (["FOREGROUND", "BACKGROUND"].includes(coerceIntoType(item.action_type))) return true;
  else if (["Wrench", "Fist", "Water Bucket", "Pocket Lighter"].includes(item.name)) return true;
  return false;
});
items = items.filter(item => item.name.indexOf("null_item") !== 0);
items = items.map(item => ({
  ...item,
  actionType: coerceIntoType(item.action_type)
}));

console.log(`~ ${items.length.toLocaleString()} total items.`);

for (const item of items) {
  let [name, extension] = item.texture.split(".");
  textureFiles.push(`${name}`);
}

textureFiles = [...new Set(textureFiles)];
const missingTextures = [];

for (const texture of textureFiles) {
  const texturePath = path.resolve(process.cwd(), `./textures/${texture}.png`);
  if (!fs.existsSync(texturePath)) missingTextures.push(texture);
  continue;
}
if (missingTextures.length) return console.log(`!! Missing textures. A total of ${missingTextures.length} textures are missing: ${missingTextures.join(", ")}.`);

const allTextures = fs.readdirSync(path.resolve(process.cwd(), "./textures"));
textureFiles = allTextures.map(filename => filename.split(".")[0]);

const textures = [];
for (const texture of textureFiles) {
  const texturePath = path.resolve(process.cwd(), `./textures/${texture}.png`);
  const $texture = {};
  $texture.data = fs.readFileSync(texturePath);
  $texture.hash = crypto.createHash("sha1")
    .update($texture.data)
    .digest("hex");
  $texture.id = snowflake.generate();
  $texture.name = `${texture}.png`;
  textures.push($texture);
}

(async function() {
  const connection = await mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD
  });
  console.log("Connected to the database, and creating the databases if it doesn't exist.");
  await connection.query('CREATE DATABASE IF NOT EXISTS `world_planner`;');
  await connection.query('USE `world_planner`;');
  await connection.query(`
  CREATE TABLE IF NOT EXISTS \`items\` (
    \`id\` VARCHAR(20),
    \`game_id\` INT SIGNED,
    \`action_type\` SMALLINT SIGNED,
    \`item_category\` SMALLINT SIGNED,
    \`name\` VARCHAR(150),
    \`texture\` VARCHAR(50),
    \`texture_hash\` CHAR(40),
    \`texture_x\` SMALLINT SIGNED,
    \`texture_y\` SMALLINT SIGNED,
    \`spread_type\` SMALLINT SIGNED,
    \`collision_type\` SMALLINT SIGNED,
    \`rarity\` SMALLINT SIGNED,
    \`max_amount\` SMALLINT SIGNED,
    \`break_hits\` SMALLINT SIGNED,
    \`override_item_data\` BOOLEAN
  );
  `);
  await connection.query(`
  CREATE TABLE IF NOT EXISTS \`textures\` (
    \`id\` BIGINT,
    \`name\` VARCHAR(50),
    \`hash\` CHAR(40),
    \`contents\` LONGBLOB
  );`);
  await connection.query(`
  CREATE TABLE IF NOT EXISTS \`weathers\` (
    \`id\` BIGINT,
    \`name\` VARCHAR(60),
    \`file\` VARCHAR(60),
    \`hash\` CHAR(40),
    \`contents\` LONGBLOB
  );`);

  const textureBeginTime = Date.now();
  for (const texture of textures) {
    const [[hashTexture]] = await connection.execute("SELECT * FROM `textures` WHERE (hash = ?)", [ texture.hash ]).catch(()=>{});
    const [[nameTexture]] = await connection.execute("SELECT * FROM `textures` WHERE (name = ?)", [ texture.name ]);
    if (hashTexture && nameTexture && hashTexture.id === nameTexture.id) {
      console.log(`[Texture Updater]: Texture ${texture.name} (${hashTexture.hash}) has been found in database with no changed; skipping..`);
    } else if (hashTexture && nameTexture && hashTexture.id !== nameTexture.id) {
      console.log(`[Texture Updater]: Conflict for texture ${texture.name} (${texture.hash}), deleting and re-storing it..`);
      await connection.query("DELETE FROM `textures` WHERE hash = ? OR name = ?", [ hashTexture.hash, nameTexture.name ]);
      await connection.query("INSERT INTO `textures` (id, name, hash, contents) VALUES (?, ?, ?, ?)", [
        BigInt(texture.id),
        texture.name,
        texture.hash,
        texture.data
      ]);
    } else if (nameTexture && !hashTexture) {
      console.log(`[Texture Updater]: The texture ${texture.name} (${texture.hash}) has been updated in the files, updating record..`);
      await connection.query("UPDATE `textures` SET contents = ?, hash = ? WHERE name = ?", [
        texture.data,
        texture.hash,
        texture.name
      ]);
    } else if (!nameTexture) {
      console.log(`[Texture Updater]: New texture ${texture.name} (${texture.hash}), inserting record..`);
      await connection.query("INSERT INTO `textures` (id, name, hash, contents) VALUES (?, ?, ?, ?)", [
        BigInt(texture.id),
        texture.name,
        texture.hash,
        texture.data
      ]);
    }
  }
  const textureEndTime = Date.now();
  const itemsBeginTime = Date.now();
  for (const item of items) {
    console.time(`item-${item.id}-${item.name.toLowerCase().replace(/'/g, "").split(" ").join("-")}`);
    const itemIdBuffer = Buffer.alloc(9);
    itemIdBuffer.writeInt16LE(Math.floor(Math.random() * 32767));
    itemIdBuffer.writeInt32BE(item.id, 2);
    itemIdBuffer.writeInt8(Math.floor(Math.random() * 20), 6);
    itemIdBuffer.writeInt16LE(Math.floor(Math.random() * 32767), 7);

    //console.log(`[Item Updater]: #${item.id}`);

    const [[dbItem]] = await connection.query(`SELECT * FROM \`items\` WHERE (game_id = ?)`, [ item.id ]);

    if (!dbItem) {
      await connection.query(`INSERT INTO \`items\` (id, game_id, action_type, item_category, name, texture,
        texture_hash, texture_x, texture_y, spread_type, collision_type, rarity, max_amount, break_hits, override_item_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
          itemIdBuffer.toString("hex"),
          item.id,
          actionTypeMap[item.actionType] || console.error(`Unknown action type ${item.actionType}.`),
          item.item_category,
          item.name,
          `${item.texture.split(".")[0]}.png`,
          textures.find(texture => texture.name === `${item.texture.split(".")[0]}.png`).hash,
          item.texture_x,
          item.texture_y,
          item.spread_type,
          item.collision_type,
          item.rarity,
          item.max_amount || 1,
          Math.floor(item.break_hits / 6) || 1,
          false
        ]);
      console.log(`[Item Updater]: Added item ${item.name} #${item.id}`);
    }

    if (dbItem && dbItem.texture_hash !== textures.find(texture => texture.name === dbItem.texture).hash) {
      await connection.query("UPDATE `items` SET texture_hash = ?, id = ? WHERE game_id = ?", [
        textures.find(texture => texture.name === `${item.texture.split(".")[0]}.png`).hash,
        itemIdBuffer.toString("hex"),
        item.id
      ]);
      console.log(`[Item Updater]: Updated texture hash info for ${item.name}.`);
    }

    if (dbItem && dbItem.name !== item.name) {
      await connection.query("UPDATE `items` SET name = ?, id = ? WHERE game_id = ?", [
        item.name,
        itemIdBuffer.toString("hex"),
        item.id
      ]);
      console.log(`[Item Updater]: Updated name info for ${item.name}.`);
    }

    if (dbItem && dbItem.max_amount !== item.max_amount) {
      await connection.query("UPDATE `items` SET max_amount = ?, id = ? WHERE game_id = ?", [
        item.max_amount,
        itemIdBuffer.toString("hex"),
        item.id
      ]);
      console.log(`[Item Updater]: Updated max amount info for ${item.name}.`);
    }

    if (dbItem && dbItem.rarity !== item.rarity) {
      await connection.query("UPDATE `items` SET rarity = ?, id = ? WHERE game_id = ?", [
        item.rarity,
        itemIdBuffer.toString("hex"),
        item.id
      ]);
      console.log(`[Item Updater]: Updated rarity info for ${item.name}.`);
    }

    if (dbItem && dbItem.item_category !== item.item_category) {
      await connection.query("UPDATE `items` SET item_category = ?, id = ? WHERE game_id = ?", [
        item.item_category,
        itemIdBuffer.toString("hex"),
        item.id
      ]);
      console.log(`[Item Updater]: Updated item category info for ${item.name}.`);
    }

    if (dbItem && dbItem.break_hits !== (Math.floor(item.break_hits / 6) || 1)) {
      await connection.query("UPDATE `items` SET break_hits = ?, id = ? WHERE game_id = ?", [
        Math.floor(item.break_hits / 6) || 1,
        itemIdBuffer.toString("hex"),
        item.id
      ]);
      console.log(`[Item Updater]: Updated break hit info for ${item.name}.`);
    }

    if (dbItem && dbItem.collision_type !== item.collision_type) {
      await connection.query("UPDATE `items` SET collision_type = ?, id = ? WHERE game_id = ?", [
        item.collision_type,
        itemIdBuffer.toString("hex"),
        item.id
      ]);
      console.log(`[Item Updater]: Updated collision type info for ${item.name}.`);
    }

    if  (dbItem && !dbItem.override_item_data) {
      if (dbItem && dbItem.action_type !== actionTypeMap[item.actionType]) {
        console.log(dbItem.action_type, actionTypeMap[item.actionType], item.actionType);
        await connection.query("UPDATE `items` SET action_type = ?, id = ? WHERE game_id = ?", [
          actionTypeMap[item.actionType],
          itemIdBuffer.toString("hex"),
          item.id
        ]);
        console.log(`[Item Updater]: Updated action type info for ${item.name}.`);
      }

      if (dbItem && dbItem.texture !== `${item.texture.split(".")[0]}.png`) {
        await connection.query("UPDATE `items` SET texture = ?, texture_hash = ?, id = ? WHERE game_id = ?", [
          `${item.texture.split(".")[0]}.png`,
          textures.find(texture => texture.name === `${item.texture.split(".")[0]}.png`).hash,
          itemIdBuffer.toString("hex"),
          item.id
        ]);
        console.log(`[Item Updater]: Updated texture info for ${item.name}.`);
      }

      if (dbItem && (dbItem.texture_x !== item.texture_x || dbItem.texture_y !== item.texture_y || dbItem.spread_type !== item.spread_type)) {
        await connection.query("UPDATE `items` SET texture_x = ?, texture_y = ?, spread_type = ?, id = ? WHERE game_id = ?", [
          item.texture_x,
          item.texture_y,
          item.spread_type,
          itemIdBuffer.toString("hex"),
          item.id
        ]);
        console.log(`[Item Updater]: Updated texture meta for ${item.name}.`);
      }
    }
    console.timeEnd(`item-${item.id}-${item.name.toLowerCase().replace(/'/g, "").split(" ").join("-")}`);
  }

  const itemsEndTime = Date.now();
  const weatherBeginTime = Date.now();

  let weathers = fs.readdirSync(path.resolve(process.cwd(), "./weathers"));
  weathers = weathers.map(filename => filename.split(".")[0]);
  const weatherObjects = [];

  for (const weather of weathers) {
    const weatherPath = path.resolve(process.cwd(), `./weathers/${weather}.png`);
    const $weather = {};
    $weather.data = fs.readFileSync(weatherPath);
    $weather.hash = crypto.createHash("sha1")
      .update($weather.data)
      .digest("hex");
    $weather.id = snowflake.generate();
    $weather.file = `${weather}.png`;
    $weather.name = weather;
    weatherObjects.push($weather);
  }

  for (const weatherObj of weatherObjects) {
    const [[hashWeather]] = await connection.execute("SELECT * FROM `weathers` WHERE (hash = ?)", [ weatherObj.hash ]).catch(()=>{});
    const [[nameWeather]] = await connection.execute("SELECT * FROM `weathers` WHERE (name = ?)", [ weatherObj.name ]);
    if (hashWeather && nameWeather && hashWeather.id === hashWeather.id) {
      console.log(`[Weather Updater]: Weather ${weatherObj.name} (${hashWeather.hash}) has been found in database with no changed; skipping..`);
    } else if (hashWeather && nameWeather && hashWeather.id !== nameWeather.id) {
      console.log(`[Weather Updater]: Conflict for weather ${weatherObj.name} (${hashWeather.hash}), deleting and re-storing it..`);
      await connection.query("DELETE FROM `weathers` WHERE hash = ? OR name = ?", [ hashWeather.hash, nameWeather.name ]);
      await connection.query("INSERT INTO `weathers` (id, name, file, hash, contents) VALUES (?, ?, ?, ?, ?)", [
        BigInt(weatherObj.id),
        weatherObj.name,
        weatherObj.file,
        weatherObj.hash,
        weatherObj.data
      ]);
    } else if (nameWeather && !hashWeather) {
      console.log(`[Weather Updater]: The weather ${weatherObj.name} (${weatherObj.hash}) has been updated in the files, updating record..`);
      await connection.query("UPDATE `weathers` SET contents = ?, hash = ? WHERE name = ?", [
        weatherObj.data,
        weatherObj.hash,
        weatherObj.name
      ]);
    } else if (!nameWeather) {
      console.log(`[Weather Updater]: New weather ${weatherObj.name} (${weatherObj.hash}), inserting record..`);
      await connection.query("INSERT INTO `weathers` (id, name, file, hash, contents) VALUES (?, ?, ?, ?, ?)", [
        BigInt(weatherObj.id),
        weatherObj.name,
        weatherObj.file,
        weatherObj.hash,
        weatherObj.data
      ]);
    }
  }
  const weatherEndTime = Date.now();
  
  console.log(`[Updater]: Took ${(textureEndTime - textureBeginTime).toLocaleString()}ms for textures to get updated.`);
  console.log(`[Updater]: Took ${(itemsEndTime - itemsBeginTime).toLocaleString()}ms for items to get updated.`);
  console.log(`[Updater]: Took ${(weatherEndTime - weatherBeginTime).toLocaleString()}ms for the weathers to get updated.`);
}());

function coerceIntoType(actionType) {
  if (actionType === 0) return "FIST";
  else if (actionType === 1) return "TOOL";
  else if ([8, 37, 44, 48, 64, 107, 121, 133, 137].includes(actionType)) return "NONE";
  else if ([18, 22, 23, 28].includes(actionType)) return "BACKGROUND";
  else if (actionType === 19) return "SEED";
  else if (actionType === 20) return "CLOTH";
  else if (actionType === 129) return "COMPONENT";
  return "FOREGROUND";
}
