import asyncio
from aiobtdht import DHT
from aioudp import UDPServer
import requests
import time
import geoip2.webservice
from sqlite3 import connect
import datetime
import os
import time
from config import geoip_account_id, geoip_license_key

numberOfTries = 0

async def checkDBFile():
    global db, cursor
    db = connect('./database.db', check_same_thread=False)
    cursor = db.cursor()


async def checkTable():
    cursor.execute("""CREATE TABLE IF NOT EXISTS peers(
        "info_hash"	TEXT,
        "name"	TEXT,
        "ip_address"	TEXT,
        "city"	TEXT,
        "city_geoname_id"	TEXT,
        "state"	TEXT,
        "state_code"	TEXT,
        "country"	TEXT,
        "country_code"	TEXT,
        "country_geoname_id"	TEXT,
        "is_in_european_union"	TEXT,
        "zip_code"	TEXT,
        "metro_code"	TEXT,
        "latitude"	TEXT,
        "longitude"	TEXT,
        "continent"	TEXT,
        "continent_code"	TEXT,
        "continent_geoname_id"	TEXT,
        "network"	TEXT,
        "isp"	TEXT,
        "organization"	TEXT,
        "accuracy_radius"	TEXT,
        "time_zone"	TEXT,
        "domain"	TEXT,
        "autonomous_system_number"	TEXT,
        "autonomous_system_organization"	TEXT,
        "is_anonymous"	TEXT,
        "is_anonymous_proxy"	TEXT,
        "is_anonymous_vpn"	TEXT,
        "is_hosting_provider"	TEXT,
        "is_legitimate_proxy"	TEXT,
        "is_public_proxy"	TEXT,
        "is_residential_proxy"	TEXT,
        "is_satellite_provider"	TEXT,
        "is_tor_exit_node"	TEXT,
        "datetime"	TEXT)
    """)


async def runUDPServerAndBootstrap(loop):
    global udp, dht
    print("Starting UDPServer...")
    udp = UDPServer()
    udp.run("0.0.0.0", 12346, loop=loop)
    print("UDPServer Started!")

    initial_nodes = [
        ("67.215.246.10", 6881), # router.bittorrent.com
        ("67.215.246.10", 8991), # router.bittorrent.com 2nd port
        ("67.215.242.138", 6881), # router.bittorrent.com
        ("67.215.242.139", 6881), # router.bittorrent.com
        ("87.98.162.88", 6881), # dht.transmissionbt.com
        ("212.129.33.59", 6881), # dht.transmissionbt.com
        ("82.221.103.244", 6881), # router.utorrent.com
        ("174.129.43.152", 6881) #dht.aelitis.com
    ]

    print("Bootstrapping...")
    dht = DHT(int("0x54A10C9B159FC0FBBF6A39029BCEF406904019E0", 16),
              server=udp, loop=loop)

    await dht.bootstrap(initial_nodes)
    time.sleep(5)
    print("Bootstrap Finished!")
    return


async def checkMagnet():
    with open('./magnet.txt', 'r') as file:
        data = file.read()
    if len(data.strip()) == 0:
        file.close()
        print("No More Magnet Links Found In Text File. Checking Again After 2 Min")
        return True
    else:
        file.close()
        return False


async def getMagnet():
    with open('./magnet.txt', 'r') as file:
        magnet = file.readline()

    global info_hash, name
    info_hash = magnet[magnet.find(
        'magnet:?xt=urn:btih:')+len('magnet:?xt=urn:btih:'):magnet.rfind('&dn')]
    print(f"Info Hash -> {info_hash}")
    quoted_name = magnet[magnet.find('&dn=')+len('&dn='):magnet.rfind('&tr=')]
    unquoted_name = requests.utils.unquote(quoted_name)
    splitted_name = unquoted_name.split("&tr=")[0]
    name = splitted_name.replace("+", " ")
    print(f"Name -> {name}")
    file.close()
    return {'info_hash': info_hash, 'name': name}


async def findPeers():
    global peers
    info = await getMagnet()
    print(f"Searching peers for {info['info_hash']}")
    peers = list(await dht[bytes.fromhex(info['info_hash'])])
    print("Peers -> ", peers)
    return {'peers': peers}


async def fetchIPInfo(ip):
    client = geoip2.webservice.AsyncClient(
        int(geoip_account_id), geoip_license_key)

    print(f"Fetching Info For IP -> {str(ip)}")
    response = await client.city(str(ip))
    ip_address = response.traits.ip_address
    city = response.city.name
    city_geoname_id = response.city.geoname_id
    state = response.subdivisions.most_specific.name
    state_code = response.subdivisions.most_specific.iso_code
    country = response.country.name
    country_code = response.country.iso_code
    country_geoname_id = response.country.geoname_id
    is_in_european_union = response.country.is_in_european_union
    zip_code = response.postal.code
    metro_code = response.location.metro_code
    latitude = response.location.latitude
    longitude = response.location.longitude
    continent = response.continent.name
    continent_code = response.continent.code
    continent_geoname_id = response.continent.geoname_id
    network = response.traits.network
    isp = response.traits.isp
    organization = response.traits.organization
    accuracy_radius = str(response.location.accuracy_radius) + " km"
    time_zone = response.location.time_zone
    domain = response.traits.domain
    autonomous_system_number = response.traits.autonomous_system_number
    autonomous_system_organization = response.traits.autonomous_system_organization
    is_anonymous = response.traits.is_anonymous
    is_anonymous_proxy = response.traits.is_anonymous_proxy
    is_anonymous_vpn = response.traits.is_anonymous_vpn
    is_hosting_provider = response.traits.is_hosting_provider
    is_legitimate_proxy = response.traits.is_legitimate_proxy
    is_public_proxy = response.traits.is_public_proxy
    is_residential_proxy = response.traits.is_residential_proxy
    is_satellite_provider = response.traits.is_satellite_provider
    is_tor_exit_node = response.traits.is_tor_exit_node

    await client.close()
    return {'ip_address': ip_address, 'city': city, 'city_geoname_id': city_geoname_id, 'state': state, 'state_code': state_code, 'country': country, 'country_code': country_code,
            'country_geoname_id': country_geoname_id, 'is_in_european_union': is_in_european_union, 'zip_code': zip_code, 'metro_code': metro_code, 'latitude': latitude,
            'longitude': longitude, 'continent': continent, 'continent_code': continent_code, 'continent_geoname_id': continent_geoname_id, 'network': network, 'isp': isp,
            'organization': organization, 'accuracy_radius': accuracy_radius, 'time_zone': time_zone, 'domain': domain, 'autonomous_system_number': autonomous_system_number,
            'autonomous_system_organization': autonomous_system_organization, 'is_anonymous': is_anonymous, 'is_anonymous_proxy': is_anonymous_proxy, 'is_anonymous_vpn': is_anonymous_vpn,
            'is_hosting_provider': is_hosting_provider, 'is_legitimate_proxy': is_legitimate_proxy, 'is_public_proxy': is_public_proxy, 'is_residential_proxy': is_residential_proxy,
            'is_satellite_provider': is_satellite_provider, 'is_tor_exit_node': is_tor_exit_node}


async def deleteMagnet():
    with open('./magnet.txt', 'r') as file:
        data = file.read().splitlines(True)
    with open('./magnet.txt', 'w') as file2:
        file2.writelines(data[1:])
    file.close()
    file2.close()
    return


async def saveIPData():
    global numberOfTries
    find_peers = await findPeers()
    peers = find_peers['peers']
    result = cursor.execute(
        "SELECT info_hash FROM peers WHERE info_hash=?", (info_hash,)).fetchone()

    if result is None:
        if len(peers) != 0:
            for ip in peers:
                peer_number = peers.index(ip) + 1
                total_peers = len(peers)

                peer = await fetchIPInfo(ip[0])
                ip_address = str(peer['ip_address'])
                city = str(peer['city'])
                city_geoname_id = str(peer['city_geoname_id'])
                state = str(peer['state'])
                state_code = str(peer['state_code'])
                country = str(peer['country'])
                country_code = str(peer['country_code'])
                country_geoname_id = str(peer['country_geoname_id'])
                is_in_european_union = str(peer['is_in_european_union'])
                zip_code = str(peer['zip_code'])
                metro_code = str(peer['metro_code'])
                latitude = str(peer['latitude'])
                longitude = str(peer['longitude'])
                continent = str(peer['continent'])
                continent_code = str(peer['continent_code'])
                continent_geoname_id = str(peer['continent_geoname_id'])
                network = str(peer['network'])
                isp = str(peer['isp'])
                organization = str(peer['organization'])
                accuracy_radius = str(peer['accuracy_radius'])
                time_zone = str(peer['time_zone'])
                domain = str(peer['domain'])
                autonomous_system_number = str(
                    peer['autonomous_system_number'])
                autonomous_system_organization = str(
                    peer['autonomous_system_organization'])
                is_anonymous = str(peer['is_anonymous'])
                is_anonymous_proxy = str(peer['is_anonymous_proxy'])
                is_anonymous_vpn = str(peer['is_anonymous_vpn'])
                is_hosting_provider = str(peer['is_hosting_provider'])
                is_legitimate_proxy = str(peer['is_legitimate_proxy'])
                is_public_proxy = str(peer['is_public_proxy'])
                is_residential_proxy = str(peer['is_residential_proxy'])
                is_satellite_provider = str(peer['is_satellite_provider'])
                is_tor_exit_node = str(peer['is_tor_exit_node'])
                current_time = str(
                    datetime.datetime.now().replace(microsecond=0))

                os.system("cls" if os.name == "nt" else "clear")
                print(f"Saving IP Info {peer_number}/{total_peers} IPs")
                cursor.execute("INSERT INTO peers (info_hash, name, ip_address, city, city_geoname_id, state, state_code, country, country_code, country_geoname_id, is_in_european_union, zip_code, metro_code, latitude, longitude, continent, continent_code, continent_geoname_id, network, isp, organization, accuracy_radius, time_zone, domain, autonomous_system_number, autonomous_system_organization, is_anonymous, is_anonymous_proxy, is_anonymous_vpn, is_hosting_provider, is_legitimate_proxy, is_public_proxy, is_residential_proxy, is_satellite_provider, is_tor_exit_node, datetime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                               (str(info_hash), str(name), ip_address, city, city_geoname_id, state, state_code, country, country_code, country_geoname_id, is_in_european_union, zip_code, metro_code, latitude, longitude, continent, continent_code, continent_geoname_id, network, isp, organization, accuracy_radius, time_zone, domain, autonomous_system_number, autonomous_system_organization, is_anonymous, is_anonymous_proxy, is_anonymous_vpn, is_hosting_provider, is_legitimate_proxy, is_public_proxy, is_residential_proxy, is_satellite_provider, is_tor_exit_node, current_time))
                db.commit()
                time.sleep(1)
            print(f"All IP Data Saved For Info Hash -> {info_hash}")
            db.close()
            numberOfTries -= numberOfTries
            await deleteMagnet()
            return
        else:
            if numberOfTries >= 2:
                numberOfTries -= numberOfTries
                print("No Peers Found Even After 3 Tries! Restarting Script...")
                os.system('pm2 restart peers')
                return
            else:
                numberOfTries += 1
                print("No Peers Found! Trying Again...")
                return
    else:
        print("Current Magnet Is Already In Database!")
        db.close()
        await deleteMagnet()
        return


async def main(loop):
    global numberOfTries
    await runUDPServerAndBootstrap(loop)
    while True:
        try:
            await checkDBFile()
            await checkTable()
            if await checkMagnet() is True:
                db.close()
                time.sleep(120)
                continue
            else:
                pass
            await saveIPData()
        except FileNotFoundError:
            print("Missing File! Trying Again After 2 Min")
            numberOfTries -= numberOfTries
            time.sleep(120)
            continue

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.run_forever()