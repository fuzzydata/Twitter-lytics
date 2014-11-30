################# BASH SCRIPT TO ENABLE NOTIFICATION IN UBUNTU - FOR CRON JOB ESPECIALLY##################
PID=$(pgrep gnome-session)
export DBUS_SESSION_BUS_ADDRESS=$(grep -z DBUS_SESSION_BUS_ADDRESS /proc/$PID/environ|cut -d= -f2-)
echo $DBUS_SESSION_BUS_ADDRESS
