FROM scupt-prerequisite:latest

ENV PATH /root/.cargo/bin/:$PATH

WORKDIR /scupt-2pc

# invalid the cache
COPY . .

ENV RUSTFLAGS="-Cinstrument-coverage"
ENV LLVM_PROFILE_FILE="scupt-2pc-%p-%m.profraw"

COPY Cargo.toml ./
COPY src ./src
COPY data ./data
COPY data/config.toml /root/.cargo/

RUN cargo build --verbose
RUN date
RUN cargo test --verbose -- --nocapture
RUN date

RUN grcov . -s . --binary-path ./target/debug/ \
    -t html --branch --ignore-not-existing -o ./target/debug/coverage/ \
RUN rm -rf /var/www/coverage
RUN cp -r ./target/debug/coverage /var/www/

RUN echo "\
server {\n\
       listen 8000;\n\
       listen [::]:8000;\n\
\n\
       server_name coverage.com;\n\
\n\
       root /var/www/coverage;\n\
       index index.html;\n\
\n\
}" > /etc/nginx/sites-available/coverage

RUN ln -sf /etc/nginx/sites-available/coverage /etc/nginx/sites-enabled/default

EXPOSE 8000

CMD ["nginx", "-g", "daemon off;"]