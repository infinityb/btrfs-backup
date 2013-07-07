all: btrfs-backup

btrfs-backup: hashbang.txt btrfs-backup.zip
	cat hashbang.txt btrfs-backup.zip > btrfs-backup
	chmod +x btrfs-backup

btrfs-backup.zip: __main__.py btrfsbackup
	zip btrfs-backup.zip -r btrfsbackup __main__.py

clean:
	rm btrfs-backup btrfs-backup.zip
