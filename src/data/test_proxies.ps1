# Configuration
$proxies = @(
    "52.143.141.88",
    "4.178.185.235",
    "20.199.91.99",
    "20.199.94.172",
    "4.251.124.194",
    "4.251.123.247",
    "4.178.175.105",
    "4.212.8.170",
    "4.251.113.113",
    "4.212.15.184",
    "4.251.116.117",
    "4.211.104.4",
    "4.211.105.36",
    "4.211.105.71",
    "4.178.189.175",
    "4.178.189.213",
    "4.178.189.160",
    "4.251.113.80"
)

# Fonction pour tester la connectivité d'une IP
function Test-ProxyConnectivity {
    param($ip)
    try {
        $result = Test-NetConnection -ComputerName $ip -Port 3128 -WarningAction SilentlyContinue -ErrorAction Stop
        return $result.TcpTestSucceeded
    }
    catch {
        return $false
    }
}

# Fonction pour vérifier si l'IP est dans des blacklists DNS
function Test-DNSBlacklist {
    param($ip)
    $blacklists = @(
        "zen.spamhaus.org",
        "bl.spamcop.net",
        "cbl.abuseat.org"
    )
    
    $reversed = ($ip -split '\.')[3..0] -join '.'
    $blacklisted = $false
    
    foreach ($bl in $blacklists) {
        try {
            $null = Resolve-DnsName -Name "$reversed.$bl" -ErrorAction Stop
            $blacklisted = $true
            Write-Host "   ❌ Listé dans $bl" -ForegroundColor Red
        }
        catch {
            Write-Host "   ✅ Clean sur $bl" -ForegroundColor Green
        }
    }
    return $blacklisted
}

# Fonction principale
function Test-Proxy {
    param($ip)
    Write-Host "`n=== Test du proxy: $ip ===" -ForegroundColor Cyan
    
    # Test de connectivité
    Write-Host "`nTest de connectivité:" -ForegroundColor Yellow
    $isConnective = Test-ProxyConnectivity $ip
    if ($isConnective) {
        Write-Host "   ✅ Port 3128 accessible" -ForegroundColor Green
    } else {
        Write-Host "   ❌ Port 3128 inaccessible" -ForegroundColor Red
    }
    
    # Test des blacklists DNS
    Write-Host "`nVérification des blacklists DNS:" -ForegroundColor Yellow
    $isBlacklisted = Test-DNSBlacklist $ip
    
    # Liens de vérification
    Write-Host "`nLiens de vérification supplémentaires:" -ForegroundColor Yellow
    Write-Host "   🔍 AbuseIPDB: https://www.abuseipdb.com/check/$ip"
    Write-Host "   🔍 IPVoid: https://www.ipvoid.com/ip-blacklist-check/"
    Write-Host "   🔍 MXToolbox: https://mxtoolbox.com/SuperTool.aspx?action=blacklist%3a$ip"
    
    # Résumé
    Write-Host "`nRésumé:" -ForegroundColor Yellow
    if ($isConnective -and -not $isBlacklisted) {
        Write-Host "   ✅ Cette IP semble utilisable" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Cette IP pourrait avoir des problèmes" -ForegroundColor Red
    }
    
    Write-Host "`n----------------------------------------"
}

# Exécution des tests
Write-Host "Début des tests de proxies..." -ForegroundColor Cyan
$totalProxies = $proxies.Count
$currentProxy = 0

foreach ($ip in $proxies) {
    $currentProxy++
    Write-Host "`nProgression: [$currentProxy/$totalProxies]" -ForegroundColor Magenta
    Test-Proxy $ip
    Start-Sleep -Seconds 1
}

Write-Host "`nTests terminés!" -ForegroundColor Green