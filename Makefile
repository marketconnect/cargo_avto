run:
	go run app/cmd/main.go

git:
	git add .
	git commit -a -m "$m"
	git push -u origin main
